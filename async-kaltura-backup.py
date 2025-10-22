import csv
import aiofiles
import requests
from lxml import etree
import asyncio
import aiohttp
import os
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta

root_path = '/path/to/archive/directory'
partner_id = ''
client_secret = ''
user_id = ''
num_concurrent = 6

api = "https://www.kaltura.com/api_v3/service"
auth = "/session/action/start"
url = "/action/getUrl"
li = "/action/list"
att = "/attachment_attachmentasset"
cap = "/caption_captionasset"
flav = "/flavorasset"

completed_requests = 0
total_assets = 0
total_urls = 0
files_downloaded = 0
unwatched = 0
unwatched_size = 0
report_rows = list()


def get_token():
    auth_params = {
        'secret': client_secret,
        'userID': user_id,
        'type': 2,  # 2 for admin, 0 for user
        'partnerId': partner_id,
        'expiry': '86400'  # in seconds (may need to increase)
    }
    response = requests.post(api + auth, data=auth_params)
    root = etree.fromstring(response.content)
    return root.find(".//result").text


async def download_file(queue, sema, session, logger):
    global files_downloaded
    while True:
        e = await queue.get()
        dl_dir = os.path.join(root_path, e['entryId'])
        if e['filename'] == '':
            clean_name = re.sub(r"[/\\?%*:|\"<>\x7F\x00-\x1F]", "-", e['name'])
            filename = os.path.join(dl_dir, clean_name + '.' + e['ext'])
        else:
            # thinking this should be clean since it's a "filename"
            filename = os.path.join(dl_dir, e['filename'])
        if not os.path.exists(dl_dir):
            os.makedirs(dl_dir)
        # uncomment next block to overwrite possibly timed-out files from previous run
        # if os.path.exists(filename):
        #     os.remove(filename)
        if not os.path.isfile(filename):
            # prevent timeout on large videos
            timeout = aiohttp.ClientTimeout(total=6400)
            async with sema:
                try:
                    async with session.get(e['url'], timeout=timeout) as res:
                        res.raise_for_status()
                        async with aiofiles.open(filename, 'wb') as f:
                            # data = await res.read()
                            # commented out and changed below to fix memory usage errors
                            async for chunk in res.content.iter_any():
                                await f.write(chunk)
                except Exception as err:
                    await logger.put(f"Error downloading {e['url']} Msg: {str(err)} Type: {type(err)}")
        files_downloaded += 1
        queue.task_done()


async def get_dl_url(queue, sema, session, token, to_queue, logger):
    global total_urls
    while True:
        e = await queue.get()
        params = {
            'ks': token,
            'id': e['id']
        }
        if e['type'] == 'flavor':
            dest = flav
        elif e['type'] == 'caption':
            dest = cap
        else:
            dest = att
        async with sema:
            try:
                async with session.post(api + dest + url, data=params) as res:
                    data = await res.content.read()
                root = etree.fromstring(data)
                dl_url = root.find('.//result').text
                if dl_url is not None:
                    await to_queue.put({
                        'entryId': e['entryId'],
                        'url': dl_url,
                        'filename': e['filename'],
                        'ext': e['ext'],
                        'name': e['name']
                    })
            except Exception as err:
                await logger.put(f'Entry: {e["id"]} ERROR: {type(err)}')
        total_urls += 1
        queue.task_done()


async def get_asset_info(queue, sema, session, token, to_queue, logger):
    global total_assets, completed_requests
    while True:
        e = await queue.get()
        params = {
            'ks': token,
            'filter[objectType]': 'KalturaAssetFilter',
            'filter[entryIdEqual]': e['entryId'],
            'pager[objectType]': 'KalturaPager'
        }
        async with sema:
            try:
                async with session.post(e['url'], data=params) as res:
                    data = await res.content.read()
                root = etree.fromstring(data)
                objects = root.find('.//objects')
                assets = [
                    {'type': e['type'],
                     'name': e['name'],
                     'id': i.find('id').text,
                     'ext': i.find('fileExt').text,
                     'entryId': i.find('entryId').text,
                     'sizeInBytes': i.find('sizeInBytes').text,
                     'filename': i.find('filename').text if i.find('filename') is not None else '',
                     'language': i.find('language').text if i.find('language') is not None else '',
                     'isOriginal': i.find('isOriginal').text if i.find('isOriginal') is not None else ''}
                    for i in objects]
                for asset in assets:
                    if e['type'] == 'flavor':
                        if asset['isOriginal'] == '1':
                            total_assets += 1
                            await to_queue.put(asset)
                    else:
                        await to_queue.put(asset)
                        total_assets += 1
                    # await logger.put(str(asset))
            except Exception as err:
                await logger.put(f'Entry: {e["entryId"]} ERROR: {type(err)}')
        completed_requests += 1
        queue.task_done()


async def get_unwatched(queue, sema, session, token, to_queue, logger):
    global unwatched
    now = datetime.now()
    four_years_ago = (now - relativedelta(years=4))
    params = dict()
    params['filter[objectType]'] = 'KalturaMediaEntryFilter'
    params['filer[orderBy]'] = '-createdAt'
    params['filter[lastPlayedAtLessThanOrEqual]'] = int(four_years_ago.timestamp())
    params['ks'] = token
    params['pager[objectType]'] = 'KalturaPager'
    params['pager[pageSize]'] = 500
    api_url = "https://www.kaltura.com/api_v3/service/baseentry/action/list"
    while True:
        e = await queue.get()
        params['pager[pageIndex]'] = e['pageIndex']
        async with sema:
            try:
                async with session.post(api_url, data=params) as res:
                    data = await res.content.read()
                root = etree.fromstring(data)
                if root.find('.//error') is None:
                    items = root.find('.//objects')
                    for item in items:
                        object_type = item.find('.//objectType').text
                        if object_type == "KalturaMediaEntry":
                            unwatched += 1
                            await to_queue.put({'Entry ID': item.find('.//id').text,
                                                'Name': item.find('.//name').text,
                                                'User': item.find('.//creatorId').text,
                                                'Reference ID': '',
                                                'Source': ''})
                        if object_type == "KalturaExternalMediaEntry":
                            unwatched += 1
                            await to_queue.put({'Entry ID': item.find('.//id').text,
                                                'Name': item.find('.//name').text,
                                                'User': item.find('.//creatorId').text,
                                                'Reference ID': item.find('.//referenceId').text,
                                                'Source': item.find('.//externalSourceType').text})
            except Exception as err:
                await logger.put(f'ERROR: {type(err)}')
        queue.task_done()


async def calc_size(queue, sema, session, token, to_queue, logger):
    global unwatched_size
    params = dict()
    params['filter[objectType]'] = 'KalturaAssetFilter'
    params['ks'] = token
    params['pager[objectType]'] = 'KalturaPager'
    api_url = "https://www.kaltura.com/api_v3/service/flavorasset/action/list"
    while True:
        e = await queue.get()
        params['filter[entryIdEqual]'] = e['Entry ID']
        async with sema:
            try:
                async with session.post(api_url, data=params) as res:
                    data = await res.content.read()
                root = etree.fromstring(data)
                if root.find('.//error') is None:
                    items = root.find('.//objects')
                    # we could snag the source flavor here and save an api call later
                    e['Total Size (KB)'] = sum([int(item.find('size').text) for item in items])
                    unwatched_size += e['Total Size (KB)']
                    await to_queue.put(e)
            except Exception as err:
                await logger.put(f'ERROR: {type(err)}')
        queue.task_done()


async def log_message(queue):
    async with aiofiles.open(os.path.join(root_path, 'log_file.txt'), 'w') as f:
        while True:
            msg = await queue.get()
            await f.write(msg + '\n')
            queue.task_done()


async def print_pre_status():
    global unwatched, unwatched_size
    while True:
        print(f'Collecting {unwatched} unwatched entries, with a total storage size of {(unwatched_size/1000.0):.2f}', end='\r')
        await asyncio.sleep(1)


async def print_status():
    global total_assets, completed_requests, total_urls, files_downloaded
    while True:
        print(f'Searching: found {total_assets} child assets, {files_downloaded} assets downloaded', end='\r')
        await asyncio.sleep(2)


async def queue_to_list(async_queue):
    global report_rows
    while True:
        row_data = await async_queue.get()
        report_rows.append(row_data)
        async_queue.task_done()


async def main():

    token = get_token()

    print(f'Collecting unwatched video data...')

    filename = f"KMC-unwatched-four-years-from-{datetime.now().strftime('%Y-%m-%d')}.csv"
    fieldnames = ['Entry ID', 'Name', 'User', 'Reference ID', 'Source', 'Total Size (KB)']

    api_sema = asyncio.Semaphore(num_concurrent)
    urls_sema = asyncio.Semaphore(num_concurrent)
    dl_sema = asyncio.Semaphore(num_concurrent)
    search_queue = asyncio.Queue()
    calc_queue = asyncio.Queue()
    report_queue = asyncio.Queue()
    init_queue = asyncio.Queue()
    url_queue = asyncio.Queue()
    download_queue = asyncio.Queue()
    log_queue = asyncio.Queue()
    session = aiohttp.ClientSession()

    # **********************************************************************
    # The first section will collect unwatched videos (maxing out at 10k)
    # calculate the approximate size of the download
    # store a manifest of base entries (csv) and prompt to continue the download

    # open the log for the entire process I've never tried any number other than 2 workers
    log_tasks = []
    for i in range(2):
        task = asyncio.create_task(log_message(log_queue))
        log_tasks.append(task)

    # API maxes out at 10k results with a max 500 item page size
    for page in range(1, 21):
        search_queue.put_nowait({'pageIndex': page})

    # create worker to log messages from prelim tasks
    prelim_print = asyncio.create_task(print_pre_status())

    search_tasks = []
    calc_tasks = []
    report_tasks = []
    for i in range(num_concurrent):
        search_task = asyncio.create_task(get_unwatched(search_queue,
                                                        api_sema,
                                                        session,
                                                        token,
                                                        calc_queue,
                                                        log_queue))
        search_tasks.append(search_task)
        calc_task = asyncio.create_task(calc_size(calc_queue,
                                                  api_sema,
                                                  session,
                                                  token,
                                                  report_queue,
                                                  log_queue))
        calc_tasks.append(calc_task)
        report_task = asyncio.create_task(queue_to_list(report_queue))
        report_tasks.append(report_task)

    await search_queue.join()
    for task in search_tasks:
        task.cancel()
    await asyncio.gather(*search_tasks, return_exceptions=True)

    await calc_queue.join()
    for task in calc_tasks:
        task.cancel()
    await asyncio.gather(*calc_tasks, return_exceptions=True)

    await report_queue.join()
    for task in report_tasks:
        task.cancel()
    await asyncio.gather(*report_tasks, return_exceptions=True)

    prelim_print.cancel()
    await asyncio.gather(prelim_print, return_exceptions=True)

    print(f'Collected {unwatched} unwatched entries, with a total storage size of {(unwatched_size / 1000.0):.2f}')

    # report_queue is now primed and we can print the report
    # Should pause here to say ok and jump to here with report maybe
    with open(filename, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_rows)

    print(f'Report saved to {filename}')


    # prime the first queue with calls for flavors, captions and attachments
    for e in report_rows:
        init_queue.put_nowait({'entryId': e['Entry ID'], 'url': api + flav + li, 'type': 'flavor', 'name': e['Name']})
        init_queue.put_nowait({'entryId': e['Entry ID'], 'url': api + cap + li, 'type': 'caption', 'name': e['Name']})
        init_queue.put_nowait({'entryId': e['Entry ID'], 'url': api + att + li, 'type': 'attachment', 'name': e['Name']})

    # create workers to log messages from other workers
    print_update = asyncio.create_task(print_status())



    # there's probably a more idiomatic way to write all these
    asset_tasks = []
    url_tasks = []
    dl_tasks = []
    for i in range(num_concurrent):
        asset_task = asyncio.create_task(get_asset_info(init_queue, api_sema, session, token, url_queue, log_queue))
        asset_tasks.append(asset_task)
        url_task = asyncio.create_task(get_dl_url(url_queue, urls_sema, session, token, download_queue, log_queue))
        url_tasks.append(url_task)
        dl_task = asyncio.create_task(download_file(download_queue, dl_sema, session, log_queue))
        dl_tasks.append(dl_task)

    await init_queue.join()
    for task in asset_tasks:
        task.cancel()
    await asyncio.gather(*asset_tasks, return_exceptions=True)

    await url_queue.join()
    for task in url_tasks:
        task.cancel()
    await asyncio.gather(*url_tasks, return_exceptions=True)

    await download_queue.join()
    for task in dl_tasks:
        task.cancel()
    await asyncio.gather(*dl_tasks, return_exceptions=True)

    # shut down the logging workers
    await log_queue.join()
    for task in log_tasks:
        task.cancel()
    await asyncio.gather(*log_tasks, return_exceptions=True)

    print_update.cancel()
    await asyncio.gather(print_update, return_exceptions=True)

    await session.close()

    # print final status to the main output
    print(f"Searching: found {total_assets} child assets, {files_downloaded} assets downloaded          ")
    print("Check log_file.txt in root backup directory for errors")
    print("Search for empty files with find /path/to/backup/directory -size 0 and verify size in KMC")


if __name__ == '__main__':
    asyncio.run(main())

import csv
import aiofiles
import requests
from lxml import etree
import asyncio
import aiohttp
import os
import re

csv_path = ''  # requires "Entry ID" column
root_path = ''
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
            timeout = aiohttp.ClientTimeout(total=3600)
            async with sema:
                try:
                    async with session.get(e['url'], timeout=timeout) as res:
                        async with aiofiles.open(filename, 'wb') as f:
                            data = await res.read()
                            await f.write(data)
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
            except Exception as err:
                await logger.put(f'Entry: {e["entryId"]} ERROR: {type(err)}')
        completed_requests += 1
        queue.task_done()


async def log_message(queue):
    async with aiofiles.open(os.path.join(root_path, 'log_file.txt'), 'w') as f:
        while True:
            msg = await queue.get()
            await f.write(msg + '\n')
            queue.task_done()


async def print_status():
    global total_assets, completed_requests, total_urls, files_downloaded
    while True:
        print(f'Requests: {completed_requests}, Assets: {total_assets}, Urls: {total_urls}, Files: {files_downloaded}', end='\r')
        await asyncio.sleep(3)


async def main():
    entries = []
    with open(csv_path, 'r') as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            entries.append(row)
    token = get_token()

    api_sema = asyncio.Semaphore(num_concurrent)
    urls_sema = asyncio.Semaphore(num_concurrent)
    dl_sema = asyncio.Semaphore(num_concurrent)
    init_queue = asyncio.Queue()
    url_queue = asyncio.Queue()
    download_queue = asyncio.Queue()
    log_queue = asyncio.Queue()
    session = aiohttp.ClientSession()

    # prime the first queue with calls for flavors, captions and attachments
    for e in entries:
        init_queue.put_nowait({'entryId': e['Entry ID'], 'url': api + flav + li, 'type': 'flavor', 'name': e['Name']})
        init_queue.put_nowait({'entryId': e['Entry ID'], 'url': api + cap + li, 'type': 'caption', 'name': e['Name']})
        init_queue.put_nowait({'entryId': e['Entry ID'], 'url': api + att + li, 'type': 'attachment', 'name': e['Name']})

    # create workers to log messages from other workers
    print_update = asyncio.create_task(print_status())

    log_tasks = []
    for i in range(2):
        task = asyncio.create_task(log_message(log_queue))
        log_tasks.append(task)

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
    print(f'Requests: {completed_requests}, Assets: {total_assets}, Urls: {total_urls}, Files: {files_downloaded}')
    print("Check log_file.txt in root backup directory for errors")

if __name__ == '__main__':
    asyncio.run(main())

import csv
import aiofiles
import requests
from lxml import etree
import asyncio
import aiohttp

partner_id = ''
client_secret = ''
user_id = ''
num_concurrent = 6
deleted = 0
errors = 0
# change to most recent report file from the archive script
csv_path = 'KMC-unwatched-four-years-from-2025-10-17.csv'


def get_token():
    auth_params = {
        'secret': client_secret,
        'userID': user_id,
        'type': 2,  # 2 for admin, 0 for user
        'partnerId': partner_id,
        'expiry': '86400'  # in seconds (may need to increase)
    }
    auth_url = "https://www.kaltura.com/api_v3/service/session/action/start"
    response = requests.post(auth_url, data=auth_params)
    root = etree.fromstring(response.content)
    return root.find(".//result").text


async def delete_entries(queue, sema, session, token, logger):
    global deleted, errors
    params = dict()
    params['ks'] = token
    api_url = "https://www.kaltura.com/api_v3/service/baseentry/action/delete"
    # api_url = "https://www.kaltura.com/api_v3/service/baseentry/action/get"  # test_url
    while True:
        e = await queue.get()
        params['entryId'] = e['id']
        async with sema:
            try:
                async with session.post(api_url, data=params) as res:
                    data = await res.content.read()
                root = etree.fromstring(data)
                if root.find('.//error') is None:
                    deleted += 1
                else:
                    code = root.find('.//code').text
                    if code == 'ENTRY_ID_NOT_FOUND':
                        # already gone
                        deleted += 1
                    elif code == 'ACTION_BLOCKED':
                        # instead of dealing with limiting concurrent threads to 35 calls per 10 seconds
                        # just let the API limit the script and put the blocked action back in the queue
                        await queue.put(e)
                    else:
                        # an actual API error we don't know
                        await logger.put(f'Failed to delete entry {e["id"]}: {code}')
                        errors += 1
            except Exception as err:
                await logger.put(f'HTTP or XML ERROR: {type(err)}')
        queue.task_done()


async def log_message(queue):
    async with aiofiles.open('deleted_log_file.txt', 'w') as f:
        while True:
            msg = await queue.get()
            await f.write(msg + '\n')
            queue.task_done()


async def print_status():
    global deleted, errors
    while True:
        print(f'Deleting {deleted} entries (Errors: {errors})', end='\r')
        await asyncio.sleep(1)


async def main():

    token = get_token()
    api_sema = asyncio.Semaphore(num_concurrent)
    queue = asyncio.Queue()
    log_queue = asyncio.Queue()
    session = aiohttp.ClientSession()

    entries = []
    with open(csv_path, 'r') as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            entries.append(row)

    print(f'CSV file contains {len(entries)} entries')

    log_tasks = []
    for i in range(2):
        task = asyncio.create_task(log_message(log_queue))
        log_tasks.append(task)

    # create worker to log messages from prelim tasks
    print_update = asyncio.create_task(print_status())

    [queue.put_nowait({'id': e['Entry ID']}) for e in entries]

    delete_tasks = []
    for i in range(num_concurrent):
        delete_task = asyncio.create_task(delete_entries(queue, api_sema, session, token, log_queue))
        delete_tasks.append(delete_task)

    await queue.join()
    for task in delete_tasks:
        task.cancel()
    await asyncio.gather(*delete_tasks, return_exceptions=True)

    await log_queue.join()
    for task in log_tasks:
        task.cancel()
    await asyncio.gather(*log_tasks, return_exceptions=True)

    print_update.cancel()
    await asyncio.gather(print_update, return_exceptions=True)

    await session.close()

    print('')
    print(f'Process deleted {deleted} entries, check deleted_log_file.txt for errors.')


if __name__ == '__main__':
    asyncio.run(main())

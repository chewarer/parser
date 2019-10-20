from time import time
import pickle
import asyncio
import aiohttp
import traceback

from parser_lib import parse_item, normalize, save_csv, write_file, error_log, filter_urls
from all_categories import get_all_items_urls

CSV_FILE_NAME = 'data/items.csv'
result = list()
error_on_urls = set()
completed_urls = set()


async def get_data_from(url):
    """Async request"""
    async with aiohttp.ClientSession() as session:
        async with session.get(normalize(url)) as resp:
            print(f'{resp.status}: {url}')
            if str(resp.status).startswith('50'):
                error_on_urls.add(url)
            elif resp.status not in (200, 301, 304):
                error_log(url, 'data/error_item.log')
                return
            data = await resp.read()
            if not data:
                return

    try:
        if data:
            html = parse_item(url, data)
            result.append(html)
            completed_urls.add(url)
    except Exception as e:
        print(f'Except on parse data: {e}. URL: {url}')
        traceback.print_tb(e.__traceback__)
        return


async def _create_task(loop, urls):
    no_concurrent = 10  # limit concurrency
    dltasks = set()

    for url in urls:
        if len(dltasks) >= no_concurrent:
            # Wait for some download to finish before adding a new one
            _done, dltasks = await asyncio.wait(
                dltasks, return_when=asyncio.FIRST_COMPLETED)
        dltasks.add(loop.create_task(get_data_from(url)))

    await asyncio.wait(dltasks)


def run_iter(urls):
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_create_task(loop, urls))
    finally:
        loop.close()


def parse_until_complete(urls):
    """
        Parse urls.
        Repeat until some urls are parsed.
    """
    start = time()
    global error_on_urls
    attempts = 10
    urls = filter_urls(set(urls), CSV_FILE_NAME)
    print(f'URLs for parsing: {len(urls)}')

    # Repeat if some urls has not be parsed
    for i in range(attempts):
        if i > 0:
            urls = list(error_on_urls)
            error_on_urls = set()  # reset set
        run_iter(urls)

        if not error_on_urls:
            break

    # Save parsed data
    try:
        with open(f'{CSV_FILE_NAME}_raw.pkl', 'wb') as f:
            f.write(pickle.dumps(result))
        save_csv(result, CSV_FILE_NAME)
    except Exception as e:
        print(e)
    finally:
        if error_on_urls:
            write_file('data/error_on_urls.txt', error_on_urls, mode='w')

    print(f'Total time: {time() - start}')
    print(f'Success completed: {len(result)} urls')
    print(f'Errors: {error_on_urls.__len__()}')


if __name__ == '__main__':
    _urls = get_all_items_urls()
    parse_until_complete(_urls)

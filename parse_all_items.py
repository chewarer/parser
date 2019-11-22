import sys
from time import time
import asyncio
import aiohttp
import traceback

from parser_lib import parse_item, normalize, save_csv, write_file, error_log, filter_urls
from all_categories import get_all_items_urls

CSV_FILE_NAME = 'data/items.csv'
result = list()
error_on_urls = set()
completed_urls = set()


async def get_response(url):
    """Async request"""
    async with aiohttp.ClientSession() as session:
        async with session.get(normalize(url)) as resp:
            print(f'{resp.status}: {url}')
            if str(resp.status).startswith('50'):
                error_on_urls.add(url)
                return
            elif resp.status not in (200, 301, 304):
                error_log(url, 'data/error_item.log')
                return
            data = await resp.read()

            return data


async def get_data_from(url):
    data = await get_response(url)
    try:
        if data:
            parsed_data = parse_item(url, data)
            result.append(parsed_data)
            completed_urls.add(url)

            # Get image
            img_url = parsed_data.get('img_url')[1:]
            if img_url:
                img = await get_response(img_url)
                if img:
                    write_file(img_url, img, mode='wb')

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


def parse_until_complete(urls, chunksize):
    """
        Parse urls.
        Repeat until some urls are parsed.
    """
    start = time()
    global error_on_urls
    attempts = 10
    urls = list(filter_urls(set(urls), CSV_FILE_NAME))
    print(f'Read urls from file: {CSV_FILE_NAME}')
    print(f'URLs for parsing: {len(urls)}')

    # Repeat if some urls has not be parsed
    for i in range(attempts):
        if i > 0:
            urls = list(error_on_urls)
            error_on_urls = set()  # reset set
        run_iter(urls[:chunksize])

        if not error_on_urls:
            break

    # Save parsed data
    try:
        save_csv(result, CSV_FILE_NAME, update=True)
    except Exception as e:
        print(e)
    finally:
        if error_on_urls:
            write_file('data/error_on_urls.txt', error_on_urls, mode='w')

    print(f'\nTotal time: {time() - start}')
    print(f'Success completed: {len(result)} urls')
    print(f'Errors: {error_on_urls.__len__()}')


if __name__ == '__main__':
    chunk_size = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    _urls = get_all_items_urls()
    parse_until_complete(_urls, chunk_size)

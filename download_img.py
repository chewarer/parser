import os
import requests
import asyncio
import traceback

from repeater import repeater
from parser_lib import normalize, write_file, read_csv
from parse_all_items import get_response

ASSET_PATH = 'assets'


def existing_files() -> set:
    """Get existing asset files"""
    list_files = list()
    for (dirpath, dirnames, filenames) in os.walk(ASSET_PATH):
        list_files += [
            os.path.join(dirpath, file)
            for file in filenames
            if os.path.getsize(os.path.join(dirpath, file)) > 0
        ]

    return set(list_files)


def all_items_images():
    """Get all item images"""
    rows = read_csv('data/items.csv')
    return {row.get('img_url')[1:] for row in rows if row.get('img_url')}


@repeater()
def get_img(url):
    resp = requests.get(normalize(url))
    if resp.status_code == 200 and resp.content:
        return resp.content


def download():
    """Download skipped images"""
    not_existing_files = all_items_images() - existing_files()

    for url in not_existing_files:
        print(f'Download file: {url}')
        data = get_img(url)
        write_file(url, data, mode='wb')


async def _get_data_from(url):
    try:
        img = await get_response(url)
        if img:
            write_file(url, img, mode='wb')

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
        dltasks.add(loop.create_task(_get_data_from(url)))

    await asyncio.wait(dltasks)


def download_async():
    """Async download skipped images"""
    loop = asyncio.new_event_loop()
    not_existing_files = all_items_images() - existing_files()
    try:
        loop.run_until_complete(_create_task(loop, not_existing_files))
    finally:
        loop.close()


if __name__ == '__main__':
    download_async()

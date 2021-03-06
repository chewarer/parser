import os
from itertools import chain

from parse_brands import get_all_menu_links_by_brands


def get_all_menu_links(url: str) -> set:
    """
        Get all urls from catalog menu
        (Categories/subcategories urls)
    """
    from parser_lib import get_url_data, normalize, read_html, write_file
    html = get_url_data(url)
    html = read_html(html)
    page_items = html.select('#nav-catalog a')

    urls = set(normalize(url.attrs.get('href')) for url in page_items)

    write_file('data/categories_urls.txt', urls, mode='w')

    return urls


def get_all_items_urls() -> set:
    """
        Get full url list for each item on site.
        Write result to files by subcategories
        and return flat list of urls
    """
    from parser_lib import write_file, all_subcategory_items, HOST
    chunks = list()
    all_items_file = 'data/all_item_urls_flat.txt'

    if os.path.exists(all_items_file):
        with open(all_items_file, 'r') as f:
            urls = set(u.strip() for u in f)
        if urls:
            return urls

    if not os.path.exists('data'):
        os.mkdir('data')

    urls = get_all_menu_links(HOST)
    for url in urls:
        print(url)
        chunks.append(all_subcategory_items(url))

    urls_brands = get_all_menu_links_by_brands(HOST)
    for url in urls_brands:
        print(url)
        chunks.append(all_subcategory_items(url, '_brands'))

    # Write items urls to file
    urls = tuple(chain(*chunks))
    print(f'total urls: {len(urls)}')
    urls = set(urls)
    print(f'total unique urls: {len(urls)}')

    write_file(all_items_file, urls)

    return urls


if __name__ == '__main__':
    get_all_items_urls()

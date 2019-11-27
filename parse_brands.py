"""
Parse brands categories tree.
Save:
    - categories tree
    - table of final subcategories with main parameters
"""
from itertools import chain
from parser_lib import get_url_data, read_html, normalize


def get_brand_menu(url: str):
    """Get brand menu"""
    html = read_html(get_url_data(url))
    return html.select_one('.main_content .order-1 #aside-nav-brands')


def last_level_urls(html) -> set:
    """Get last level urls from menu"""
    from parser_lib import normalize

    urls_level_last = html.select('li.levelLast a')
    urls_level_last = set(normalize(url.attrs.get('href')) for url in urls_level_last)

    return urls_level_last


def find_subcategory(url) -> list:
    """
    Find subcategory from the URL.
    :return: Last level urls
    """
    html = read_html(get_url_data(url))
    menu = html.select('.cats_div a.product-type__desc')
    menu_urls = set(normalize(url.attrs.get('href')) for url in menu)

    if menu_urls:
        return list(chain(*(find_subcategory(url) for url in menu_urls)))

    # The last level url (base case)
    return [url]


def get_all_menu_links_by_brands(host: str) -> set:
    url = f'{host}/brands/iek'

    html_menu = get_brand_menu(url)
    last_urls = last_level_urls(html_menu)

    return set(chain(*(find_subcategory(url) for url in last_urls)))

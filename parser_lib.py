import os
from copy import copy

import requests
import json
import shutil
import csv
import ast
from itertools import chain
from tempfile import NamedTemporaryFile
from requests import ReadTimeout
from urllib.parse import urlparse, urlunparse
from urllib.error import HTTPError

from user_agent import generate_user_agent
from bs4 import BeautifulSoup
from bs4.element import Tag

from config import SCHEME, HOSTNAME
from repeater import repeater
from parse_subcategories import parse_html

HOST = urlunparse((SCHEME, HOSTNAME, '', '', '', ''))
HEADERS = {'User-Agent': generate_user_agent(device_type="desktop", os=('mac', 'linux'))}


def all_subcategory_items(url):
    """
        Get items urls by subcategory url
    """
    html = get_url_data(url)
    if not html:
        return
    html = read_html(html)

    # Parse subcategory pages.
    # Get title, url, breadcrumbs, filtered parameters.
    row = parse_html(html, url)
    if row:
        save_csv([row], 'data/categories.csv', update=True)

    page_items = (
            html.select('.catalog-item .catalog-item_title a')
            or html.select('.catalog-item_offer a.catalog-item_titlelink')
    )
    pager = html.select('.pagination a.page span')

    # maybe needed parse here

    if pager:
        for page in range(int(pager[0].text), int(pager[-1].text) + 1):
            html = get_url_data(url, params={'page': page})
            html = read_html(html)
            if html:
                items = (
                        html.select('.catalog-item .catalog-item_title a')
                        or html.select('.catalog-item_offer a.catalog-item_titlelink')
                )
                page_items += items

    items_urls = [normalize(x.attrs.get('href')) for x in page_items]

    # Write items urls to file by category
    fname = urlparse(url).path
    fname = fname[1:] if fname.startswith('/') else fname
    fname = fname.replace('/', '---')

    path = 'data/parsed_categories/'
    if not os.path.exists(path):
        os.mkdir(path)
    fname = path + fname
    write_file(fname, items_urls)

    return items_urls


def normalize(url):
    url = urlparse(url)
    return urlunparse((SCHEME, HOSTNAME, url.path, url.params, url.query, url.fragment))


def denormalize(url):
    url = urlparse(url)
    return urlunparse(('', '', url.path, url.params, url.query, url.fragment))


@repeater()
def get_url_data(url, params=None):
    """Download page from url"""
    try:
        html = requests.get(normalize(url), params=params, headers=HEADERS, timeout=30)
        print(f'{html.status_code}: {html.url}')
    except HTTPError as e:
        print(f'Error on url {url}: {e}')
    except ReadTimeout as e:
        print(f'Error on url {url}: {e}')
    else:
        return html.text


def read_html(html):
    return BeautifulSoup(html, 'html.parser')


def write_file(filename, rows, mode='a'):
    """Save data to file"""
    dirname, fname = os.path.split(filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    with open(filename, mode) as f:
        if mode.lower() == 'wb':
            f.write(rows)

        else:
            for row in rows:
                f.write(f'{row}\n')


def error_log(mes, error_file='errors.txt'):
    write_file(error_file, (mes,))


def parse_item(url, html=None) -> dict:
    """Parse item page"""
    html = read_html(html)
    if not html:
        return {}

    item = dict()
    item['header'] = (html.select_one('.container h1').text or '').strip().replace('\n', '')
    img_url = (html.select_one('.product_img_link').attrs.get('href') or '').strip()
    item['img_url'] = denormalize(normalize(img_url))

    autotext = (html.select_one('div.autotext').text or '').strip().replace('\n', '')
    item['autotext'] = f'<p>{autotext}</p>' if autotext else None

    item['product_warning__text'] = (html.select_one('.product-warning__text').text or ''
                                     ).strip().replace('\n', '')
    item['url_orig'] = url

    breadcrumb_top = (html.select('.top_row ul.breadcrumb li a')[1:]
                      or html.select('ul.breadcrumb li a')[1:])
    item['breadcrumb_top'] = '|'.join((u.text for u in breadcrumb_top))

    breadcrumb_bottom = html.select('.bottom_row ul.breadcrumb li a')
    item['breadcrumb_bottom'] = '|'.join((u.text for u in breadcrumb_bottom))

    item['breadcrumb_top_urls'] = ','.join([u.attrs.get('href') for u in breadcrumb_top])
    item['breadcrumb_bottom_urls'] = ','.join([u.attrs.get('href') for u in breadcrumb_bottom])

    price = html.select_one('.catalog-item-price .pi_value span')
    item['price'] = (price.text if price else '').strip().replace(' ', '')

    pack_price = html.select_one('.catalog-item-priceup .pi_value span')
    item['pack_price'] = (pack_price.text if pack_price else '').strip().replace(' ', '')

    prodict_param_html = html.select_one('.product_brand_info')
    for attr in ('class', 'itemprop', 'itemscope', 'itemtype'):
        prodict_param_html = delete_attr(copy(prodict_param_html), attr)
    item['prodict_param_html'] = str(prodict_param_html).replace('\n', '')

    prodict_param = html.select('.product_brand_info .product-param')
    item['prodict_param'] = json.dumps({
        (p.select_one('.product-param__name').text or '').replace(':', ''):
        (p.select_one('.product-param__desc').text or '').strip()
        for p in prodict_param
    })

    tech_params = html.select('.tech_params .tech_params_line')
    tech_params_html = html.select_one('.tech_outer .tech_params')
    item['tech_params_html'] = str(delete_attr(copy(tech_params_html), 'class')).replace('\n', '')

    item['tech_params'] = json.dumps({
        (p.select_one('.tech_params_name').text or '').replace(':', ''):
        (p.select_one('.tech_params_desc').text or '').strip()
        for p in tech_params
    })

    return item


def save_csv(items, file_name, update=True):
    """Save items to csv"""
    tempfile = NamedTemporaryFile(mode='w', delete=False)
    fieldnames = items[0].keys()

    if update and os.path.exists(file_name):
        with open(file_name, 'r') as csvfile, tempfile:
            reader = csv.DictReader(csvfile, fieldnames, delimiter=';')
            writer = csv.DictWriter(tempfile, fieldnames, delimiter=';')
            # Rewrite existing rows
            for row in reader:
                writer.writerow(row)

            # Write new rows
            for row in items:
                writer.writerow(row)
            shutil.move(tempfile.name, file_name)

    else:
        with open(file_name, 'w') as f:
            writer = csv.DictWriter(f, fieldnames, delimiter=';')
            writer.writeheader()
            for row in items:
                writer.writerow(row)


def read_csv(filename: str, fieldnames: tuple = None, delimiter: str = ';') -> tuple:
    """Read csv as dict"""
    with open(filename, 'r') as f:
        reader = csv.DictReader(f, fieldnames, delimiter=delimiter)
        data = tuple(row for row in reader)

    return data


def flat_url_list(dirname):
    """Make flat unique url list"""
    urls = set()
    for file in os.scandir(dirname):
        with open(file, 'r') as f:
            urls.update({l.strip() for l in f})

    return urls


def filter_urls(urls, csv_path):
    """Exclude from parsing URLs which already exists in CSV file"""
    if not os.path.exists(csv_path):
        print(f'CSV file is not exists: {csv_path}')
        return urls

    with open(csv_path, 'r') as f:
        c = csv.DictReader(f, delimiter=';')
        csv_urls = set(i.get('url_orig') for i in c)

    return urls - csv_urls


def delete_attr(html: Tag, attr_name):
    """Delete attribute from html"""
    if html.__class__.__name__ == 'Tag':
        html.attrs.pop(attr_name, None)
        if html.__len__():
            for child in html:
                delete_attr(child, attr_name)

    return html


def map_params() -> dict:
    """
        Create map for categories key parameters.
        Like:
            map = {
                'url': {
                    'category name': ['feature 1', 'feature 2']
                }
            }
    """
    categories_list = read_csv('data/categories.csv')

    return {
        row.get('url'): {row.get('title'): ast.literal_eval(row.get('params'))}
        for row in categories_list
    }


def flat_params() -> set:
    """Get flat list of categories key parameters"""
    categories_list = read_csv('data/categories.csv')

    params = set(chain(*(
        tuple(ast.literal_eval(row.get('params'))) for row in categories_list
    )))
    params.discard('Цена')

    return params


def flat_prod_params() -> set:
    """
        Get flat list of product params i.e. (brand, sku, category).
    """
    items = (
        json.loads(row.get('prodict_param', {})).keys()
        for row in read_csv('data/items.csv')
    )
    return set(chain(*items))

import os
import requests
import json
import shutil
import csv
from tempfile import NamedTemporaryFile
from requests import ReadTimeout
from urllib.parse import urlparse, urlunparse
from urllib.error import HTTPError

from user_agent import generate_user_agent
from bs4 import BeautifulSoup

from config import SCHEME, HOSTNAME
from repeater import repeater

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

    page_items = (
            html.select('.catalog-item .catalog-item_title a')
            or html.select('.catalog-item_offer a.catalog-item_titlelink')
    )
    pager = html.select('.pagination a.page span')

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
    with open(filename, mode) as f:
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
    item['img_url'] = (html.select_one('.product_img_link').attrs.get('href') or '').strip()

    autotext = (html.select_one('div.autotext').text or '').strip().replace('\n', '')
    item['autotext'] = f'<p>{autotext}</p>' if autotext else None

    item['product_warning__text'] = (html.select_one('.product-warning__text').text or ''
                                     ).strip().replace('\n', '')
    item['url_orig'] = url

    breadcrumb_top = html.select('.top_row ul.breadcrumb li a')
    item['breadcrumb_top'] = '|'.join((u.text for u in breadcrumb_top))

    breadcrumb_bottom = html.select('.bottom_row ul.breadcrumb li a')
    item['breadcrumb_bottom'] = '|'.join((u.text for u in breadcrumb_bottom))

    item['breadcrumb'] = None
    if not all((breadcrumb_top, breadcrumb_bottom)):
        breadcrumb = html.select('ul.breadcrumb li a')[1:]
        item['breadcrumb'] = '|'.join((u.text for u in breadcrumb))

    prodict_param = html.select('.product_brand_info .product-param')
    tech_params = html.select('.tech_params .tech_params_line')

    item['prodict_param'] = json.dumps({
        (p.select_one('.product-param__name').text or '').replace(':', ''):
        (p.select_one('.product-param__desc').text or '').strip()
        for p in prodict_param
    })

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
            reader = csv.DictReader(csvfile, fieldnames)
            writer = csv.DictWriter(tempfile, fieldnames)
            # Rewrite existing rows
            for row in reader:
                writer.writerow(row)

            # Write new rows
            for row in items:
                writer.writerow(row)
            shutil.move(tempfile.name, file_name)

    else:
        with open(file_name, 'w') as f:
            writer = csv.DictWriter(f, fieldnames)
            writer.writeheader()
            for row in items:
                writer.writerow(row)


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
        c = csv.DictReader(f)
        csv_urls = set(i.get('url_orig') for i in c)

    return urls - csv_urls

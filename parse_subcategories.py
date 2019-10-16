from parser_lib import get_url_data, read_html, save_csv, delete_class_attr, HOST
from all_categories import get_all_menu_links


def parse_html(url: str) -> dict:
    html = read_html(get_url_data(url))
    if not html:
        return {}

    item = dict()

    params = html.select('.filter .card .card-header a')
    if not params:
        return {}

    item['title'] = (html.select_one('h1').text or '').strip()

    breadcrumb = html.select_one('ul.breadcrumb')

    for attr in ['class', 'itemtype', 'itemscope', 'itemprop', 'title', 'content']:
        breadcrumb = delete_class_attr(breadcrumb, attr)

    item['breadcrumb'] = breadcrumb
    item['url'] = url
    item['params'] = [(p.text or '').rsplit(':', 1)[0].strip() for p in params]

    return item


def collect_data(urls):
    items = list()

    for url in urls:
        row = parse_html(url)
        if row:
            items.append(row)

    save_csv(items, 'data/categories.csv', update=False)

    return items


if __name__ == '__main__':
    _urls = get_all_menu_links(HOST)
    collect_data(_urls)

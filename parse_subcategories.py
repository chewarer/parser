"""
    Parse subcategory pages.
    Get title, url, breadcrumbs, filtered parameters.
"""
# from parser_lib import HOST
from all_categories import get_all_menu_links


def parse_html(html, url: str) -> dict:
    from parser_lib import delete_attr
    if not html:
        return {}

    item = dict()

    params = html.select('.filter .card .card-header a')
    if not params:
        return {}

    item['title'] = (html.select_one('h1').text or '').strip()

    breadcrumb = html.select_one('ul.breadcrumb')

    for attr in ['class', 'itemtype', 'itemscope', 'itemprop', 'title', 'content']:
        breadcrumb = delete_attr(breadcrumb, attr)

    item['breadcrumb'] = breadcrumb
    item['url'] = url
    item['params'] = [(p.text or '').rsplit(':', 1)[0].strip() for p in params]

    return item


def collect_data(urls):
    from parser_lib import get_url_data, read_html, save_csv

    items = list()

    for url in urls:
        html = read_html(get_url_data(url))
        row = parse_html(html, url)
        if row:
            items.append(row)

    save_csv(items, 'data/categories.csv', update=False)

    return items


if __name__ == '__main__':
    """
        Deprecated. 
        This task running in the 'all_subcategory_items()'
    """
    from parser_lib import HOST
    _urls = get_all_menu_links(HOST)
    collect_data(_urls)

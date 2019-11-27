"""
    Parse subcategory pages.
    Get title, url, breadcrumbs, filtered parameters.
"""


def parse_html(html, url: str) -> dict:
    from parser_lib import delete_attr
    if not html:
        return {}

    item = dict()

    params = html.select_one('.catalog-item_tech')
    params = params.select('.tech_params_name') if params else []

    title = html.select_one('h1')
    item['title'] = (title.text or '').strip() if title else None

    breadcrumb = html.select_one('ul.breadcrumb')

    for attr in ['class', 'itemtype', 'itemscope', 'itemprop', 'title', 'content']:
        breadcrumb = delete_attr(breadcrumb, attr)

    item['url'] = url
    item['params'] = [(p.text or '').rsplit(':', 1)[0].strip() for p in params]
    item['breadcrumb'] = breadcrumb

    return item

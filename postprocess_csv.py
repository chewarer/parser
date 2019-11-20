import json
import csv

from parser_lib import read_csv, save_csv, flat_params, map_params, flat_prod_params


def category_id() -> dict:
    """
    Get map:
        category url: category id
    """
    with open('data/exportcatalog.csv', 'r') as f:
        reader = csv.DictReader(f, delimiter=';')
        return {
            row['OriginalLInk']: {'id': row['id'], 'pagetitle': row.get('pagetitle'),
                                  'parent': row.get('parent')}
            for row in reader if row['id'] != 'id'
        }


def main(split=True):
    """Rebuild CSV with additional information"""

    filename_new = 'data/items_ext.csv'

    rows = read_csv('data/items.csv')
    mapper = map_params()
    all_keys = list(rows[0].keys())

    # Set category name by last breadcrumb element
    all_keys.extend((*flat_prod_params(), 'cat_1_id', 'cat_2_id', *flat_params()))

    category_ids = category_id()

    for row in rows:
        # Set default value for all keys
        for k in all_keys:
            row.setdefault(k, '')

        parent1_url = row.get('breadcrumb_top_urls', '').split(',')[-1]
        parent2_url = row.get('breadcrumb_bottom_urls', '').split(',')[-1]

        # Set category ids
        row['cat_1_id'] = category_ids.get(parent1_url, {}).get('id')
        row['cat_2_id'] = category_ids.get(parent2_url, {}).get('id')

        # Set key params to columns specified for parent category
        keys = mapper.get(parent2_url, {}).values()
        keys = set(*keys) if keys else set()
        keys.discard('Цена')
        params = json.loads(row.get('tech_params', '')) or {}

        # Set tech parameters to column
        for k in keys:
            row[k] = params.get(k)

        #  Set prodict parameters to columns
        for k, v in json.loads(row.get('prodict_param', {})).items():
            row[k] = v

        # Delete redundant keys
        row.pop('prodict_param', None)
        row.pop('tech_params', None)

    # Write new csv
    save_csv(rows, filename_new, update=False)


if __name__ == '__main__':
    main()

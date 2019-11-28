import json
import csv
import os
import shutil
import sys

from parser_lib import read_csv, save_csv, flat_params, map_params, flat_prod_params


def category_id(by_key='original-link') -> dict:
    """
    Get map:
        category url: category id
    """
    with open('data/exportcatalog.csv', 'r') as f:
        reader = csv.DictReader(f, delimiter=';')
        return {
            row[by_key]: {'id': row['id'], 'parent': row.get('parent')}
            for row in reader if row['id'] != 'id'
        }


def split_to_files(rows):
    """Split rows by category ID"""
    with open('data/exportcatalog.csv', 'r') as f:
        reader = csv.DictReader(f, delimiter=';')
        ids = {row.get('id'): [] for row in reader if row['id'] != 'id'}
        ids[None] = []

    params = flat_params() | flat_params('_brands')

    for row in rows:
        cat_1_id = row.get('cat_1_id')
        # delete extra parameters
        keys = params - row['keys']
        if cat_1_id:
            for k in keys:
                row.pop(k, None)
                row.pop('keys', None)
        ids[cat_1_id].append(row)

    dirname = 'data/items_ext'
    if os.path.exists(dirname):
        shutil.rmtree(dirname, ignore_errors=True)
    os.mkdir(dirname)

    for _id, items in ids.items():
        if items:
            save_csv(items, f'{dirname}/{_id}.csv', update=False)


def main(split=False):
    """Rebuild CSV with additional information"""

    filename_new = 'data/items_ext.csv'

    rows = read_csv('data/items.csv')
    mapper = map_params()
    mapper.update(map_params('_brands'))
    all_keys = list(rows[0].keys())

    # Set category name by last breadcrumb element
    all_keys.extend((
        *flat_prod_params(),
        'cat_1_id',
        'cat_2_id',
        'keys',
        *(flat_params() | flat_params('_brands'))
    ))

    category_ids_by_url = category_id(by_key='original-link')
    category_ids_by_name = category_id(by_key='longtitle')

    for row in rows:
        # Set default value for all keys
        for k in all_keys:
            row.setdefault(k, '')

        parent1_url = row.get('breadcrumb_top_urls')
        parent1_url = parent1_url.split(',') if parent1_url else [None]

        parent2_url = row.get('breadcrumb_bottom_urls')
        parent2_url = parent2_url.split(',') if parent2_url else [None]

        # Set category ids
        row['cat_1_id'] = category_ids_by_url.get(parent1_url[-1], {}).get('id')
        row['cat_2_id'] = category_ids_by_url.get(parent2_url[-1], {}).get('id')

        if not row['cat_1_id']:
            parent1_name = row.get('breadcrumb_top')
            parent1_name = parent1_name.split('|') if parent1_name else [None]
            row['cat_1_id'] = category_ids_by_name.get(parent1_name[-1], {}).get('id')

        if not row['cat_2_id']:
            parent2_name = row.get('breadcrumb_bottom')
            parent2_name = parent2_name.split('|') if parent2_name else [None]
            row['cat_2_id'] = category_ids_by_name.get(parent2_name[-1], {}).get('id')

        # Set key params to columns specified for parent category from bottom breadrumbs
        keys = mapper.get(parent2_url[-1], {}).values()
        if not keys and len(parent2_url) > 1:
            keys = mapper.get(parent2_url[-2], {}).values()

        # Set key params to columns specified for parent category from top breadrumbs
        if not keys:
            keys = mapper.get(parent1_url[-1], {}).values()
            if not keys and len(parent1_url) > 1:
                keys = mapper.get(parent1_url[-2], {}).values()

        keys = set(*keys) if keys else set()
        if split:
            row['keys'] = keys
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

    if split:
        split_to_files(rows)


if __name__ == '__main__':
    is_split = True if len(sys.argv) > 1 and sys.argv[1] == 'split' else False
    main(split=is_split)

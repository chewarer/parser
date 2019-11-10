import json
from parser_lib import read_csv, save_csv, flat_params, map_params, flat_prod_params


def main():
    """Rebuild CSV with additional information"""

    fname_new = 'data/items_ext.csv' \
                ''
    rows = read_csv('data/items.csv')
    mapper = map_params()
    all_keys = list(rows[0].keys())

    # Set category name by last breadcrumb element
    all_keys.extend((*flat_prod_params(), 'category_name', *flat_params()))

    for row in rows:
        # Set default value for all keys
        for k in all_keys:
            row.setdefault(k, '')

        # Set key params to columns specified for parent category
        row['category_name'] = row.get('breadcrumb_bottom', '').split('|')[-1]
        key = row.get('breadcrumb_urls', '').split(',')[-1]
        keys = mapper.get(key, {}).values()
        keys = set(*keys) if keys else set()
        keys.discard('Цена')
        params = json.loads(row.get('tech_params', '')) or {}

        # Set key parameters to column
        for k in keys:
            row[k] = params.get(k)

        #  Set prodict param to columns
        for k, v in json.loads(row.get('prodict_param', {})).items():
            row[k] = v

    # Write new csv
    save_csv(rows, fname_new, update=False)


if __name__ == '__main__':
    main()

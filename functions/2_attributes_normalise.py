import json
import csv
import pandas
import re


def synonyms_replace(string):

    dict = {
        "b&q": "own label",
        "wickes": "own label",
        "selco": "own label",
        "screwfix": "own label",
    }

    for i in dict:
        # print(i)
        if i in string:
            string = string.replace(i, dict[i])

    string = string.replace('  ', ' ')
    string = re.sub('\d{13}', "", string).strip()
    return string


def units(string):

    string = str(string)
    # print(string)

    dict = {
        'mm': 'mm',
        'cm': 'mm',
        'm': 'mm',
        'mtr': 'mm',
        'g': 'g',
        'kg': 'g',
        'ml': 'ml',
        'l': 'ml',
        'ltr': 'ml'
    }

    # millileters, grams, millimeters
    x = re.search('^(\d+\.?\d*) ?(ml|g|mm)$', string, re.IGNORECASE)
    if x:
        return format(float(x.group(1)), '.2f' ) + dict[x.group(2).lower()]

    # litres, kilograms, metres
    x = re.search('^(\d+\.?\d*) ?(l|ltr|kg|m|mtr)$', string, re.IGNORECASE)
    if x:
        return format(float(x.group(1)) * 1000, '.2f') + dict[x.group(2).lower()]

    # centimeters
    x = re.search('^(\d+\.?\d*) ?(cm)$', string, re.IGNORECASE)
    if x:
        return format(float(x.group(1)) * 10, '.2f') + dict[x.group(2).lower()]
    else:
        return string




def normalise(retailer, date, brand, colour, weight):

    data = pandas.read_csv('/home/jake/Documents/matching/bigquery_attribute_files/' + retailer + '_' + date + '.csv')
    print(data)
    headers = list(data)

    for i in range(len(data)):
    # for i in range(1000): # For testing
    #     print(i)
        product = {}
        if i == 0:
            append_write = "w"
        else:
            append_write = "a"

        for header in headers:

            if header == "sku_1":
                product['sku_1'] = data.loc[i, header]

            if header == "description":
                product['description'] = synonyms_replace(data.loc[i, header].lower())

            if re.search(brand, header, re.IGNORECASE) and not pandas.isnull(data.loc[i, header]):
                product['brand'] = synonyms_replace(data.loc[i, header].lower())

            if re.search(colour, header, re.IGNORECASE) and not pandas.isnull(data.loc[i, header]):
                product['colour'] = data.loc[i, header]

            if re.search('(diameter|length|height)', header, re.IGNORECASE) and not pandas.isnull(data.loc[i, header]):
                product['length'] = units(data.loc[i, header])

            if re.search('width', header, re.IGNORECASE) and not pandas.isnull(data.loc[i, header]):
                product['width'] = units(data.loc[i, header])

            if re.search('(thickness|depth)', header, re.IGNORECASE) and not pandas.isnull(data.loc[i, header]):
                product['thickness'] = units(data.loc[i, header])

            if re.search('material', header, re.IGNORECASE) and not pandas.isnull(data.loc[i, header]):
                product['material'] = data.loc[i, header]

            if re.search(weight, header, re.IGNORECASE) and not pandas.isnull(data.loc[i, header]):
                product['weight'] = units(data.loc[i, header])

            if re.search('quantity', header, re.IGNORECASE) and not pandas.isnull(data.loc[i, header]):
                product['pack_quantity'] = data.loc[i, header]


        with open('/home/jake/Documents/matching/bigquery_attribute_files/attr_norm_' + retailer + "_" + date + '.csv', append_write) as new_file:
            csv_headers = ['sku_1', 'description', 'brand', 'length', 'width', 'thickness', 'colour', 'material',
                           'weight', 'pack_quantity']
            writer = csv.DictWriter(new_file, csv_headers)
            if i == 0:
                writer.writeheader()

            writer.writerow(product)

normalise("wickes", "20190801", brand="brand_name", colour="^colour$", weight="weight")

normalise("bq", "20190801", brand="brand", colour="^colour$", weight="weight")
import pandas
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
import csv

retailer_1 = "wickes"
retailer_2 = "bq"
date = "20190801"



a = pandas.read_csv('/home/jake/Documents/matching/bigquery_attribute_files/attr_norm_' + retailer_1 + '_' + date + '.csv')
b = pandas.read_csv('/home/jake/Documents/matching/bigquery_attribute_files/attr_norm_' + retailer_2 + '_' + date + '.csv')

a = a
b = b

a_choices = []

for i in range(len(a)):
    a_choices.append([a.loc[i, 'description'], a.loc[i, 'sku_1']])

b_choices = []

for i in range(len(b)):
    b_choices.append([b.loc[i, 'description'], b.loc[i, 'sku_2']])

del a, b

# with open('/home/jake/Documents/matching/bigquery_attribute_files/block_' + retailer_1 + "_" + retailer_2 + '.csv',
#           'w') as new_file:
#     writer = csv.writer(new_file)
#     csv_headers = ['sku_1', 'a_description', 'b_description', 'sku_2', 'score']
#     writer.writerow(csv_headers)

b_descriptions = [x[0] for x in b_choices]

# for i in range(len(a_choices)):
for i in range(15394, len(a_choices)):
    print(i)
    a_description = a_choices[i][0]
    a_sku = a_choices[i][1]
    process_item = process.extract(a_description, b_descriptions, limit=6, scorer=fuzz.token_sort_ratio)
    # print(process_item)
    for j in process_item:
        b_match = j[0]
        score = j[1]
        for k in b_choices:
            if k[0] == b_match:
                b_sku = k[1]
        # b_sku = 0
        line = [a_sku, a_description, b_match, b_sku, score]
        # print(line)

        with open('/home/jake/Documents/matching/bigquery_attribute_files/block_' + retailer_1 + "_" + retailer_2 + '.csv',
                  'a') as new_file:
            csv_headers = ['sku_1', 'a_description', 'b_description', 'sku_2', 'score']
            writer = csv.writer(new_file)
            writer.writerow(line)

# data = pandas.read_csv('/home/jake/Documents/matching/bigquery_attribute_files/block_' + retailer_1 + "_" + retailer_2 + '.csv')
# data = data[['a_sku', 'b_sku']]
# data = pandas.MultiIndex.from_frame(data)



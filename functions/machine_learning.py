import pandas
import os
import recordlinkage
from recordlinkage.index import SortedNeighbourhood
from recordlinkage.base import BaseIndexAlgorithm

retailer_1 = "wickes"
retailer_2 = "bq"
date = "20190801"


# a = load_data('dataset_a.csv')
# b = load_data('dataset_b.csv')


a = pandas.read_csv('/home/jake/Documents/matching/bigquery_attribute_files/attr_norm_' + retailer_1 + "_" + date + '.csv')
b = pandas.read_csv('/home/jake/Documents/matching/bigquery_attribute_files/attr_norm_' + retailer_2 + "_" + date + '.csv')

c = pandas.read_csv('/home/jake/Documents/matching/raw_attribute_files/wickes_20190821.csv', dtype=str)
d = pandas.read_csv('/home/jake/Documents/matching/raw_attribute_files/bq_20190821.csv', dtype=str)

# print(a)
# print(b)
print(c)
print(d)

# data = pandas.read_csv('/home/jake/Documents/matching/bigquery_attribute_files/block_' + retailer_1 + "_" + retailer_2 + '.csv')
# data = data[['sku_1', 'sku_2']]
# pairs = pandas.MultiIndex.from_frame(data)
# print(pairs)


index = pandas.MultiIndex.from_product([c, d])


# Comparison step
compare = recordlinkage.Compare()

#
compare.string('description', 'description', label="description")
compare.string('brand', 'brand', label="brand")
compare.string('colour', 'colour', label="colour")
compare.string('length', 'length', label="length")
compare.string('width', 'width', label="width")
compare.string('thickness', 'thickness', label="thickness")
compare.string('material', 'material', label="material")
compare.string('weight', 'weight', label="weight")
compare.exact('pack_quantity', 'pack_quantity', label="pack_quantity")



# compare.string('brand', 'brand', threshold=0.7, label="brand")


features = compare.compute(pairs, a, b)
# changing index cols with rename()
features.to_csv('comparison_index.csv')

print(features)
# mfeatures = features[features.sum(axis=1) >= 3]

merge = features.merge(a, left_index=True, right_index=True, suffixes=('_match', '_wickes')).merge(b, left_index=True, right_index=True)
merge.to_csv('dataaaa.csv')
print(merge)

# compare.exact('given_name', 'given_name', label='given_name')
# compare_cl.string('surname', 'surname', threshold=0.85, label='surname')
# compare_cl.exact('date_of_birth', 'date_of_birth', label='date_of_birth')
# compare_cl.exact('suburb', 'suburb', label='suburb')
# compare_cl.exact('state', 'state', label='state')
# compare_cl.string('address_1', 'address_1', threshold=0.85, label='address_1')
#
# features = compare.compute(candidate_links, a, b)
# print(features)

# The comparison vectors
# compare_vectors = compare.compute(candidate_links, a, b)
# print(compare_vectors)
#
#
# # # Classification step
# matches = compare_vectors[compare_vectors.sum(axis=1) > 0]
# print(matches)
# print(matches)
# print(len(matches))
#
# features.describe()





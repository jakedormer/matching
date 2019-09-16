import json
import re
import csv
from datetime import datetime

#Comment for git, added more

retailer = "bq"
date = "20190801"
lookup_dict = {} # Used for creating a lookup between the new key value and the old key value

# Load in raw attribute file.
with open('/home/jake/Documents/matching/raw_attribute_files/' + retailer + '.json') as json_file:
	data = json.load(json_file)


# Create unique set of keys


def header_remover(string):
	bad_characters = ['[', ']', '\\',  ',']
	for i in bad_characters:
		# print(string)
		string = string.replace(i, '')

	string = string.replace('²', '2').replace('³', '3')
	string = re.sub('\W+', ' ', string)
	string = string.replace(" ", "_")

	return string

def value_remover(string): # Needed because of the bloody lists that scrapy creates.
	string = str(string)
	bad_characters = ['[', ']', '\\', ',', "'", '"', "  ", "\n"]
	for i in bad_characters:
		# print(string)
		string = string.replace(i, '')

	return string




r1_keys = set() # Keys with changes made to names

for item in data:
	for key in item:
		# Remove bad characters
		new_key = header_remover(key) # Remove bad characters
		r1_keys.add(new_key)
		lookup_dict[new_key] = key # Create a lookup dictionary so that you can reference back after

# Write unique keys to a JSON Object
print(lookup_dict)
json_schema = []

for key in r1_keys:

	if key == "date":
		dict_type = "DATE"
	else:
		dict_type = "STRING"
	json_item = {
		"description": key,
		"mode": "NULLABLE",
		"name": key,
		"type": dict_type
	}
	json_schema.append(json_item)

# Create Schema file using json_schema list. To be used for bigquery ingestion.
with open('/home/jake/Documents/matching/bigquery_attribute_files/' + retailer + "_" + date +'_schema.json', 'w') as schema_file:
	json.dump(json_schema, schema_file, sort_keys=True, indent=4)



# for item in data:
# data = data[:10]  # For testing
print(len(data))
counter = 1
for item in data:
	product_dict = {}
	# print(item['sku_1'])
	for key in r1_keys:
		try:
			product_dict[key] = value_remover(item[lookup_dict[key]])
		except KeyError:
			product_dict[key] = None

	with open('/home/jake/Documents/matching/bigquery_attribute_files/' + retailer + "_" + date + '.csv', 'a') as new_file:
		w = csv.DictWriter(new_file, product_dict.keys())

		if counter == 1:
			w.writeheader()
			print(product_dict.keys())

		w.writerow(product_dict)

	counter += 1


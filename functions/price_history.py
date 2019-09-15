import csv
import re
from datetime import datetime


def regex_promo(shelf_price, promotion):
    # print("£",shelf_price,", ", promotion)
    # x for £xx
    x = re.search('(?!.*?each)([0-9]+) for £(\d*\.?\d*).*', promotion, re.IGNORECASE)
    if x:
        x1 = round(float(x.group(2)) / float(x.group(1)), 2)
        # print(x1)
        return x1

    # x or more for £x.xx?
    x = re.search('.*[0-9]+ or more for £(\d*\.?\d*).*', promotion, re.IGNORECASE)
    if x:
        x1 = round(float(x.group(1)), 2)
        # print(x1)
        return x1

    # £x.xx each when you buy x or more
    x = re.search('.*£(\d*\.?\d*) each', promotion, re.IGNORECASE)
    if x:
        x1 = round(float(x.group(1)), 2)
        # print(x1)
        return x1

    # Buy 1 get 1 free
    x = re.search('(buy (one|1) get (one|1) free|bogof)', promotion, re.IGNORECASE)
    if x:
        x1 = round(shelf_price / 2, 2)
        # print(x1)
        return x1

    # x for (the price of)? x
    x = re.search('.*([3-5]) for (the price of )?([2-4]).*', promotion, re.IGNORECASE)
    if x:
        if x.group(3):
            x1 = round(shelf_price * (float(x.group(3)) / float(x.group(1))), 2)
            # print(x1)
            return x1
        else:
            x1 = round(shelf_price * (float(x.group(2)) / float(x.group(1))), 2)
            # print(x1)
            return x1

    # xx% off when you spend £x +
    x = re.search('([0-9]+)% off.*when you spend.* £(\d*\.?\d*).*', promotion, re.IGNORECASE)
    if x:
        if shelf_price > float(x.group(2)):
            x1 = round(shelf_price * (1 - float(x.group(1)) / 100), 2)
            # print(x1)
            return x1
        else:
            # print(shelf_price)
            return shelf_price

    # £x off when you spend £x +
    x = re.search('£(\d*\.?\d*).*when you spend.*£(\d*\.?\d*).*', promotion, re.IGNORECASE)
    if x:
        if shelf_price > float(x.group(2)):
            x1 = round(shelf_price - float(x.group(1)), 2)
            # print(x1)
            return x1
        else:
            # print(shelf_price)
            return shelf_price

    # Buy 1 get 1 half price
    x = re.search('buy (one|1).*half price.*', promotion, re.IGNORECASE)
    if x:
        x1 = round(shelf_price * 0.75, 2)
        # print(x1)
        return x1

    # xx% off, discount applied at checkout
    x = re.search('([0-9]+)% off.*checkout.*', promotion, re.IGNORECASE)
    if x:
        x1 = round(shelf_price * (1 - float(x.group(1)) / 100), 2)
        # print(x1)
        return x1
    else:
        # print(shelf_price)
        return shelf_price


retailer = 'wickes'

with open ('wickes.csv') as file:
    reader = csv.reader(file)
    d_reader = csv.DictReader(file)
    headers = d_reader.fieldnames
    print(headers)
    print(len(headers))
    len = int(len(headers)/ 2)
    print(len)

    # Determine how many files we will need to create.

    for i in range(len):
        print(i)
        date = headers[2 * i + 2]

        date = re.search('- (.*)', date).group(1)
        print(date)
        date_object = datetime.strptime(date, '%d/%m/%Y')
        gcs_date = date_object.strftime('%Y-%m-%d')
        yyyymmdd = date_object.strftime('%Y%m%d')
        print(yyyymmdd)

        # Open file and append headers.
        with open(retailer + "/" + retailer + "_" + yyyymmdd + '.csv', 'w') as write_file:
            writer = csv.writer(write_file)
            writer.writerow(['date', 'sku_1', 'sku_2', 'shelf_price', 'promo_price', 'promotion'])

        # Append data

        with open('wickes.csv') as file:
            reader = csv.reader(file)
            next(reader)

            for row in reader:

                sku_1 = row[0]

                # print(sku_1)
                sku_2 = ''
                promotion = row[i*2 + 2]
                shelf_price = row[i * 2 + 1]

                if sku_1 != "" and shelf_price != '':
                    shelf_price = float(row[i * 2 + 1])
                    promo_price = regex_promo(shelf_price, promotion)


                    with open(retailer + "/" + retailer + "_" + yyyymmdd + '.csv', 'a') as file_2:
                        data_row = [gcs_date, sku_1, sku_2, shelf_price, promo_price, promotion]
                        # print(data_row)
                        writer = csv.writer(file_2)
                        writer.writerow(data_row)

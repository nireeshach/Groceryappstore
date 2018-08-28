" importing data from excel to mongo db"

import os
import sys
from datetime import datetime
from optparse import OptionParser

import pymongo
import pandas as pd
from pandas import ExcelFile
import numpy as np


class UploadConverstions(object):
    """ UploadIngredients Class """

    def __init__(self, conversion_file, collection_name='', out_file="csv"):
        """
        Creating mongo connection and calling upload_ingredients method
        directly from initialization method
        """
        self.start_time = datetime.now()
        self.collection_name = collection_name
        self.conversion_file = conversion_file
        self.out_file = out_file
        if not os.path.isfile(self.conversion_file):
            print "File {} doesn't exist".format(self.conversion_file)
            sys.exit()

        self.db = self.get_mongo_conn()
        self.upload_convertions()

    def get_mongo_conn(self):
        """Gets mongo db connection"""
        MONGO_URI = 'mongodb://saran:Saran1!@ds113736.mlab.com:13736/ingredientmaster'
        client = pymongo.MongoClient(MONGO_URI)
        db = client.get_default_database()
        return db

    def parse_data(self, df, sheet_name):
        data = []
        df = df.replace(np.NaN, '', regex=True)
        df['cup per unit'] = df['cup per unit'].astype("str")
        df['cup per unit'] = df['cup per unit'].apply(lambda x: round(float(x), 3) if x else 0)
        df[['ingredient','prep', 'size', 'form']] = df[['ingredient','prep', 'size', 'form']].apply(lambda x: x.str.lower())

        l = {}
        for i,row in df.iterrows():
            #d1 = row.to_dict()
            d = l.setdefault(row['ingredient'],{})
            p = d.setdefault(row['prep'],{})
            p.setdefault(row['size'],[]).append(row['cup per unit'])
            p['form'] = row['form']
            p['category'] = sheet_name

        for ingredient, ing_dict in l.items():
            for prep, prep_dict in ing_dict.items():
                category = prep_dict.pop('category')
                form = prep_dict.pop('form')
                for size, counts in prep_dict.items():
                    cup_per_unit = round(np.mean(counts),3)
                    record = {
                            'ingredient': ingredient,
                            'category': category,
                            'form': form,
                            'preparation': prep,
                            'size': size,
                            'cup_per_unit': cup_per_unit
                    }
                    data.append(record)
        return data

    def upload_convertions(self):
        """
        Main function which reads input file(xls, csv, tsv are only supported for now)
        and loads the data into db
        """
        data = []
        ext = os.path.splitext(self.conversion_file)[-1].replace('.', '')
        if 'xls' in ext:
            xls = pd.ExcelFile(self.conversion_file)
            for sheet_name in xls.sheet_names:
                df = xls.parse(sheet_name)
                data.extend(self.parse_data(df, sheet_name))
        else:
            print("File format not supported only xls files are supported")
            sys.exit()

        if self.out_file == 'csv':
            df = pd.DataFrame(data, columns=['ingredient','preparation','size','form','category','cup_per_unit'])
            df.to_csv('ingredient_conversion.csv', index = False, encoding='utf-8')
        else:
            del_many_count = db[self.collection_name].delete_many({}).deleted_count
            ins_many_count = len(db[self.collection_name].insert_many(data).inserted_ids)
            msg = "Collection: {}, Deleted Records: {}, Uploaded Records: {}\n"
            print msg.format(self.collection_name, del_many_count, ins_many_count)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option(
        "-c", "--collection", dest="collection_name", \
        default="ingredient_conversion", \
        help="Collection name to load data"
    )
    parser.add_option(
        "-o", "--out-file", dest="out_file", \
        default="mongo", \
        help="Output File Format, Ex mongo or csv"
    )
    parser.add_option(
        "-f", "--conversion-file", dest="conversion_file", \
        metavar="FILE", \
        help="Ingredients Conversion file"
    )

    (options, args) = parser.parse_args()
    UploadConverstions(
        options.collection_name,
        options.conversion_file,
        options.out_file
    )

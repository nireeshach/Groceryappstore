"""
Loads Ingredient details into ingredientmaster database from xls or csv or tsv
"""

import os
import re
import sys
from datetime import datetime
from optparse import OptionParser

import pandas as pd
import numpy as np
import pymongo

from utils import xencode

class UploadIngredients(object):
    """ UploadIngredients Class """

    def __init__(self, cmd_options):
        """
        Creating mongo connection and calling upload_ingredients method
        directly from initialization method
        """
        self.start_time = datetime.now()
        self.source = cmd_options.collection_name
        self.ingredients_file = cmd_options.ingredients_file
        if not os.path.isfile(self.ingredients_file):
            print("File {} doesn't exist".format(self.ingredients_file))
            sys.exit()

        self.db = self.get_mongo_conn()
        self.upload_ingredients()

    def get_mongo_conn(self):
        """Gets mongo db connection"""
        MONGO_URI = 'mongodb://saran:Saran1!@ds113736.mlab.com:13736/ingredientmaster'
        client = pymongo.MongoClient(MONGO_URI)
        db = client.get_default_database()
        return db

    def upload_to_mongo(self, df):
        """Loads the pandas dataframe data into mongo db"""
        # Replacing NaN values by empty string
        df = df.replace(np.nan, '', regex=True)
        if len(df.keys().tolist()) == 0:
            return

        data_key = df.keys().tolist()[0]
        collection_name = re.sub(' +', ' ', data_key).strip()
        collection_name = data_key.replace(' ', '_').lower()
        ignore_re_patterns = ['?', '(', ')', '.']
        values = []
        for i, row in df.iterrows():
            data = {"_id": i}
            data['value'] = xencode(row[0]).strip().lower()

            if 'alternatives' in row:
                alt_str = row['alternatives'].strip()
            else:
                alt_str = ''

            alternatives = []
            if alt_str and alt_str not in ignore_re_patterns:
                alternatives = [i.strip().lower() for i in alt_str.split('; ') if i]
            data['alternatives'] = alternatives

            if 'category' in row:
                data['category'] = row['category'].strip().lower()
            if 'state' in row:
                data['state'] = row['state'].strip().lower()
            if 'group' in row:
                data['group'] = row['group'].strip().lower()
            if 'type_of_measure' in row:
                data['type_of_measure'] = row['type_of_measure'].strip().lower()
            if 'unit_of_measure' in row:
                data['unit_of_measure'] = [i.strip().lower() for i in \
                                    row['unit_of_measure'].split('; ') if i]
            values.append(data)
        del_many_count = self.db[collection_name].delete_many({}).deleted_count
        ins_many_count = len(self.db[collection_name].insert_many(values).inserted_ids)
        msg = "Collection: {}, Deleted Records: {}, Uploaded Records: {}\n"
        print(msg.format(collection_name, del_many_count, ins_many_count))

    def upload_ingredients(self):
        """
        Main function which reads input file(xls, csv, tsv are only supported for now)
        and loads the data into ingredientMaster db
        """
        ext = os.path.splitext(self.ingredients_file)[-1].replace('.', '')
        ignore_sheets = ["Database Schema", "ingredient old"]
        if 'xls' in ext:
            xls = pd.ExcelFile(self.ingredients_file)
            for sheet_name in xls.sheet_names:
                if sheet_name in ignore_sheets:
                    continue
                df = xls.parse(sheet_name)
                self.upload_to_mongo(df)
        elif 'csv' in ext:
            df = pd.read_csv(self.ingredients_file)
            self.upload_to_mongo(df)
        elif 'tsv' in ext:
            df = pd.read_csv(self.ingredients_file, sep='\t')
            self.upload_to_mongo(df)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option(
        "-c", "--collection", dest="collection_name", \
        default="", \
        help="Collection name to load data, \
        the file should have a tab with the same name"
    )
    parser.add_option(
        "-f", "--ingredients-file", dest="ingredients_file", \
        metavar="FILE", \
        help="Ingredients file"
    )

    (options, args) = parser.parse_args()
    UploadIngredients(options)

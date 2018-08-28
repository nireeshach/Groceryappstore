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


class UploadValidSkus(object):
    """ UploadValidSkus Class """

    def __init__(self, cmd_options):
        """
        Creating mongo connection and calling upload_ingredients method
        directly from initialization method
        """
        self.start_time = datetime.now()
        self.source = cmd_options.collection_name
        self.ingredients_file = cmd_options.ingredients_file
        if not os.path.isfile(self.ingredients_file):
            print "File {} doesn't exist".format(self.ingredients_file)
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
        collection_name = "valid_skus"
        ignore_re_patterns = ['?', '(', ')', '.']
        values = []
        for indx, row in df.iterrows():
            row_dict = row.to_dict()
            row_dict['_id'] = indx + 1
            values.append(row_dict)
        del_many_count = self.db[collection_name].delete_many({}).deleted_count
        ins_many_count = len(
            self.db[collection_name].insert_many(values).inserted_ids)
        msg = "Collection: {}, Deleted Records: {}, Uploaded Records: {}\n"
        print msg.format(collection_name, del_many_count, ins_many_count)

    def upload_ingredients(self):
        """
        Main function which reads input file(xls, csv, tsv are only supported for now)
        and loads the data into ingredientMaster db
        """
        ext = os.path. splitext(self.ingredients_file)[-1].replace('.', '')
        if 'xls' in ext:
            xls = pd.ExcelFile(self.ingredients_file)
            for sheet_name in xls.sheet_names:
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
        "-c", "--collection", dest="collection_name",
        default="",
        help="Collection name to load data, \
        the file should have a tab with the same name"
    )
    parser.add_option(
        "-f", "--ingredients-file", dest="ingredients_file",
        metavar="FILE",
        help="Ingredients file"
    )

    (options, args) = parser.parse_args()
    UploadValidSkus(options)

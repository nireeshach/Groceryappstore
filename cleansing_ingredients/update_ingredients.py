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
        self.category_file = cmd_options.category_file
        if not os.path.isfile(self.ingredients_file):
            print "File {} doesn't exist".format(self.ingredients_file)
            sys.exit()

        if self.category_file and not os.path.isfile(self.category_file):
            print "File {} doesn't exist".format(self.category_file)
            sys.exit()

        self.db = self.get_mongo_conn()
        self.upload_ingredients()

    def get_mongo_conn(self):
        """Gets mongo db connection"""
        MONGO_URI = 'mongodb://saran:Saran1!@ds113736.mlab.com:13736/ingredientmaster'
        client = pymongo.MongoClient(MONGO_URI)
        db = client.get_default_database()
        return db

    def get_category_dict(self, df):
        category_dict = {}
        if df.empty:
            return category_dict

        # Replacing NaN values by empty string
        df = df.replace(np.nan, '', regex=True)
        if len(df.keys().tolist()) == 0:
            return category_dict

        if 'Ingredient' not in df.keys() or 'category' not in df.keys():
            return category_dict

        ignore_re_patterns = ['?', '(', ')', '.']
        df = self.convert_df_tolower(df)
        for indx, row in df.iterrows():
            ing = row['Ingredient']
            cat = row['category']
            if not cat or cat in ignore_re_patterns:
                continue

            if 'alternatives' in row:
                alt_str = row['alternatives'].strip()
            else:
                alt_str = ''

            alternatives = ''
            if alt_str and alt_str not in ignore_re_patterns:
                alternatives = ";".join(i.strip().lower() for i in alt_str.split('; ') if i)

            category_dict[ing] = (cat, alternatives)

        return category_dict

    def convert_df_tolower(self, df):
        for column in df.columns:
            df[column] = df[column].apply(lambda x: x.lower().strip())
        return df

    def upload_to_mongo(self, df, category_dict):
        """Loads the pandas dataframe data into mongo db"""
        # Replacing NaN values by empty string
        df = df.replace(np.nan, '', regex=True)
        if len(df.keys().tolist()) == 0:
            return

        df = self.convert_df_tolower(df)
        data_key = df.keys().tolist()[0]
        new_ingredients = set(category_dict.keys()) - set(df['Ingredient'].tolist())
        new_data = []
        for new_ingredient in new_ingredients:
            catg, alts = category_dict[new_ingredient]
            row = {
                "Ingredient": new_ingredient,
                "alternatives": alts,
                "category": catg
            }
            new_data.append(row)

        if len(new_data) > 0:
            print "Added new %s rows" % len(new_data)
            new_df = pd.DataFrame(new_data)
            df = df.append(new_df, ignore_index=True)

        collection_name = re.sub(' +', ' ', data_key).strip()
        collection_name = data_key.replace(' ', '_').lower()
        key_values = df[data_key].tolist()
        ignore_re_patterns = ['?', '(', ')', '.']
        categories = []
        values = []
        for i, row in df.iterrows():
            v = xencode(row['Ingredient'])
            exs_catg = ''
            if 'category' in row:
                exs_catg = row['category'].lower().strip()

            new_catg, new_alts = category_dict.get(v, ('', ''))
            if not new_catg:
                categories.append(exs_catg)
            elif not exs_catg and new_catg:
                categories.append(new_catg)
            elif exs_catg != new_catg:
                categories.append(new_catg)
            else:
                categories.append(exs_catg)

        df['category'] = categories
        df.to_csv('Ingredients_New.csv', sep='\t', encoding='utf-8', index=False)
        '''
        alternatives_dict = dict(zip(key_values, alternatives))
        for k, v in df[data_key].to_dict().items():
            if not v.strip:
                continue

            org_v = v
            v = xencode(v)
            value_dict = {
                "_id": k,
                "value": v.strip(),
                "alternatives": alternatives_dict[org_v],
                "category": category_dict.get(v, '')
            }
            values.append(value_dict)
        del_many_count = self.db[collection_name].delete_many({}).deleted_count
        ins_many_count = len(self.db[collection_name].insert_many(values).inserted_ids)
        msg = "Collection: {}, Deleted Records: {}, Uploaded Records: {}\n"
        print msg.format(collection_name, del_many_count, ins_many_count)
        '''

    def upload_ingredients(self):
        """
        Main function which reads input file(xls, csv, tsv are only supported for now)
        and loads the data into ingredientMaster db
        """

        category_df = pd.DataFrame()
        if self.category_file:
            ext = os.path. splitext(self.category_file)[-1].replace('.', '')
            if 'csv' in ext:
                category_df = pd.read_csv(self.category_file)
            elif 'tsv' in ext:
                category_df = pd.read_csv(self.category_file, sep='\t')
        category_dict = self.get_category_dict(category_df)

        ext = os.path. splitext(self.ingredients_file)[-1].replace('.', '')
        if 'xls' in ext:
            xls = pd.ExcelFile(self.ingredients_file)
            for sheet_name in xls.sheet_names:
                df = xls.parse(sheet_name)
                self.upload_to_mongo(df, category_dict)
        elif 'csv' in ext:
            df = pd.read_csv(self.ingredients_file)
            self.upload_to_mongo(df, category_dict)
        elif 'tsv' in ext:
            df = pd.read_csv(self.ingredients_file, sep='\t')
            self.upload_to_mongo(df, category_dict)

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
    parser.add_option(
        "-g", "--category-file", dest="category_file", \
        metavar="FILE", default="",\
        help="Ingredient Category File"
    )

    (options, args) = parser.parse_args()
    UploadIngredients(options)

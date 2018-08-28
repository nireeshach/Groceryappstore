"""
Loads Ingredient details into ingredientmaster database(postgresql) from xls or csv or tsv
"""

import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from optparse import OptionParser

BASEDIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(BASEDIR)

import django
import pandas as pd
import numpy as np

#Settingup Django to access Models
PROJECT = os.path.split(BASEDIR)[-1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "%s.settings"%PROJECT)
django.setup()

from masterdata.models import Characteristic, CharacteristicType, \
                              Ingredient, State, Category, Group


class UploadIngredients(object):
    """ UploadIngredients Class """

    def __init__(self, ingredients_file):
        """
        Initialization method
        """
        self.start_time = datetime.now()
        self.ingredients_file = ingredients_file
        if not os.path.isfile(self.ingredients_file):
            print("File {} doesn't exist".format(self.ingredients_file))
            sys.exit()

        self.upload_ingredients()

    def basic_cleaning(self, val):
        """Does basic cleaning of text and return cleaned text"""
        return re.sub(' +', ' ', val).strip()

    def get_or_create_object(self, model, data):
        """Gets or Creates Django Model objects and return object"""
        return model.objects.get_or_create(**data)

    def update_or_create(self, model, distinct_values, defaults):
        """Updates or Creates Django Model objects and return object"""
        obj, iscreated = model.objects.update_or_create(
            **distinct_values,
            defaults=defaults
        )
        return obj, iscreated

    def upload_data(self, df):
        """Loads the pandas dataframe data into postgresql db"""
        # Replacing NaN values by empty string
        df = df.replace(np.nan, '', regex=True)
        if len(df.keys().tolist()) == 0:
            return

        ignore_re_patterns = ['?', '(', ')', '.']
        df.columns = [x.lower() for x in df.columns]
        df = df.apply(lambda x: x.astype(str).str.lower())
        data_key = df.keys().tolist()[0]
        _type = self.basic_cleaning(data_key).replace(' ', '_').strip()
        start_time = datetime.now()
        print(
            "Started Processing data of {}, TotalRecords: {}, starttime: {}".format(
                _type, len(df), str(start_time)
            )
        )
        for index, row in df.iterrows():
            row_data = row.to_dict()
            type_value = self.basic_cleaning(row_data.get(_type, ''))
            if not type_value or type_value in ignore_re_patterns:
                continue

            alternates = self.basic_cleaning(row_data.get('alternatives', ''))
            if not alternates:
                alternates = self.basic_cleaning(row_data.get('alternates', ''))

            alternates = [i.strip() for i in alternates.split(';') if i.strip()]
            if _type == 'ingredient':
                updated_data = {"category": None, "state": None, "group": None}
                category = self.basic_cleaning(row_data.get("category", ""))
                if category:
                    catg_obj, is_created = self.get_or_create_object(
                        Category,
                        {"name": category}
                    )
                    updated_data["category"] = catg_obj

                state = self.basic_cleaning(row_data.get("state", ""))
                if state:
                    state_obj, is_created = self.get_or_create_object(
                        State,
                        {"name": state}
                    )
                    updated_data["state"] = state_obj

                group = self.basic_cleaning(row_data.get("group", ""))
                if group:
                    group_obj, is_created = self.get_or_create_object(
                        Group,
                        {"name": group}
                    )
                    updated_data["group"] = group_obj

                done_str = self.basic_cleaning(row_data.get('done', ''))
                if done_str == 'done':
                    validated = True
                else:
                    validated = False
                updated_data["validated"] = validated

                shelflife = self.basic_cleaning(row_data.get('shelflife', ''))
                try:
                    updated_data['shelflife'] = int(float(shelflife))
                except ValueError:
                    pass

                notes = self.basic_cleaning(row_data.get('notes', ''))
                if notes:
                    updated_data['notes'] = notes

                model_obj, is_created = self.update_or_create(
                    Ingredient,
                    {"name": type_value},
                    updated_data
                )

                # Adding Alternates
                alternate_objs = []
                for alternate in alternates:
                    alt_data = {
                        "name": alternate,
                    }
                    _updated_data = dict(updated_data)
                    _updated_data["isalternate"] = True
                    alt_obj, is_created = self.update_or_create(
                        Ingredient,
                        alt_data,
                        _updated_data
                    )
                    alternate_objs.append(alt_obj)

                model_obj.alternates.clear()
                if len(alternate_objs) > 0:
                    model_obj.alternates.add(*alternate_objs)

                # Adding Finalspec values
                finalspec_objs = []
                finalspec_values = self.basic_cleaning(row_data.get('finalspec', ''))
                finalspec_values = [i.strip() for i in finalspec_values.split(';') if i.strip()]
                for finalspec in finalspec_values:
                    try:
                        fs_obj = Ingredient.objects.get(name = finalspec)
                        finalspec_objs.append(fs_obj)
                    except Ingredient.DoesNotExist:
                        print("Failed to get Ingredient with finalspec: %s" % finalspec)
                        continue
                model_obj.finalspec.clear()
                if len(finalspec_objs) > 0:
                    model_obj.finalspec.add(*finalspec_objs)
            else:
                updated_data = {"alternates": alternates}
                type_obj, is_created = self.get_or_create_object(
                    CharacteristicType,
                    {"name": _type}
                )
                updated_data["type"] = type_obj

                if _type == "state_uom_chart":
                    additional_info = {
                        "type_of_measure": row_data.get("type_of_measure", ""),
                        "unit_of_measure": row_data.get("unit_of_measure", ""),
                    }
                    updated_data["additional_info"] = additional_info

                model_obj, is_created = self.update_or_create(
                    Characteristic,
                    {"name": type_value},
                    updated_data
                )

            self.inserted_count[_type] += 1
        print(
            "Completed Processing data {}, TotalTime: {}".format(
                _type, str(datetime.now() - start_time)[:-3]
            )
        )

    def upload_ingredients(self):
        """
        Main function which reads input file(xls, csv, tsv are only supported for now)
        and loads the data into ingredientMaster db
        """

        start_time = datetime.now()
        ext = os.path. splitext(self.ingredients_file)[-1].replace('.', '')
        self.inserted_count = defaultdict(int)
        if 'xls' in ext:
            ignore_sheets = ["Database Schema", "length"]
            xls = pd.ExcelFile(self.ingredients_file)
            for sheet_name in xls.sheet_names:
                sheet_name = sheet_name.strip()
                #To check and ignore Ingredient backup sheets
                bsheets = re.match("ingredient\s(.*?)$", sheet_name, re.IGNORECASE)
                if sheet_name in ignore_sheets or bsheets:
                    continue
                print("Processing Sheet {}".format(sheet_name))
                df = xls.parse(sheet_name)
                self.upload_data(df)
        elif 'csv' in ext:
            df = pd.read_csv(self.ingredients_file)
            self.upload_data(df)
        elif 'tsv' in ext:
            df = pd.read_csv(self.ingredients_file, sep='\t')
            self.upload_data(df)

        print(
            "Completed Inserting data, Details: {}, TotalTime: {}".format(
             self.inserted_count, str(datetime.now() - start_time)[:-3]
        ))

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option(
        "-f", "--ingredients-file", dest="ingredients_file",
        metavar="FILE",
        help="Ingredients file"
    )

    (options, args) = parser.parse_args()
    UploadIngredients(options.ingredients_file)

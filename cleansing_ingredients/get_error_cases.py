"""
Tokenizes actual ingredient text, using ingredientMaster  db collections
"""

import os
import json
from datetime import datetime

import pymongo

from utils import get_master_mongo_conn

BASEDIR = os.path.dirname(os.path.realpath(__file__))
DATADIR = os.path.join(BASEDIR, 'data')
if not os.path.isdir(DATADIR):
    os.mkdir(DATADIR)


class TokenizeIngredients(object):
    """Tokenizes ingredient text"""

    def __init__(self):
        """
        Creating mongo connection and calling main method
        directly from initialization method
        """
        self.start_time = datetime.now()
        self.master_db = get_master_mongo_conn()
        self.clean_ingredients()

    def clean_ingredients(self):
        """
        This is the main function where the tokenization starts
        """
        query_params = {}

        recipes = self.master_db['cleansed_ingredients'].find(
            query_params,
            no_cursor_timeout=True
        )
        total_records_count = recipes.count()

        special_cases = []
        sand_cases = []
        error_cases = []
        for i, recipe in enumerate(recipes):
            for ing in recipe['tokenized_ingredients']:
                if 'ingredient' not in ing:
                    error_cases.append(ing)
                elif ing['ingredient'] .startswith('and/') or ing['ingredient'] .startswith('and /'):
                    sand_cases.append(ing)
                elif 'and/' in ing['ingredient'] or 'and /' in ing['ingredient']:
                    special_cases.append(ing)

        self.master_db.client.close()
        if special_cases:
            file_name = os.path.join(DATADIR, 'specialCases.json')
            with open(file_name, 'w') as f:
                f.write(json.dumps(special_cases))
        if sand_cases:
            file_name = os.path.join(DATADIR, 'specialCasesAnd.json')
            with open(file_name, 'w') as f:
                f.write(json.dumps(sand_cases))
        if error_cases:
            file_name = os.path.join(DATADIR, 'withoutIngrident.json')
            with open(file_name, 'w') as f:
                f.write(json.dumps(error_cases))


if __name__ == '__main__':
    TokenizeIngredients()

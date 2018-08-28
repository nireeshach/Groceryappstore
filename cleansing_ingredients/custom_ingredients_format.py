import os
import json
from collections import defaultdict
from datetime import datetime
from optparse import OptionParser

import pandas as pd
import pymongo
import nltk
from nltk.corpus import stopwords

from utils import xencode, get_logger

BASEDIR = os.path.dirname(os.path.realpath(__file__))
DATADIR = os.path.join(BASEDIR, 'data')
if not os.path.isdir(DATADIR):
    os.mkdir(DATADIR)

logger = get_logger('custom_ingredients_format.log')


def _print_array(data):
    """Prints array in readable format"""
    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(data)


def get_master_mongo_conn():
    """Gets mongo db connection to chefd-ingredients-research"""
    MONGO_URI = 'mongodb://saran:Saran1!@ds141165-a0.mlab.com:41165/chefd-ingredients-research'
    client = pymongo.MongoClient(MONGO_URI)
    db = client.get_default_database()
    return db


def collection_to_json(collection_name):
    start_time = datetime.now()
    logger.info(
        "Started process, Starttime: %s",
        str(start_time),
    )

    stop_words = set(stopwords.words('english'))

    db = get_master_mongo_conn()
    db_start_time = datetime.now()
    recipes = db[collection_name].find(
        {},
        no_cursor_timeout=True
    )

    total_records_count = recipes.count()
    records_to_parse = total_records_count
    logger.info(
        "Time taken to get recipes from db: %s, TotalRecord: %s",
        str(datetime.now() - db_start_time)[:-3], total_records_count
    )

    extracted_data = []
    unknown_formats = defaultdict(int)
    catg_skipped_recipes = 0
    ing_skipped_recipes = 0
    unk_skipped_recipes = 0
    outfile = os.path.join(DATADIR, 'cleansed_ingredients_filtered.json')
    with open(outfile, 'w') as f:
        for recipe in recipes:
            records_to_parse -= 1
            # Skipping recipes with alcohol beverage in ingredients
            chefd_category = recipe.get('chefd_category', '')
            if chefd_category and 'alcohol' in chefd_category:
                catg_skipped_recipes += 1
                continue

            # Skipping recipes with multiple ingredinets Ex: ingredient_2 etc
            ingredient_keys = [v for i in recipe['tokenized_ingredients']
                               for v in i if ('ingredient_' in v and
                                              'ingredient_long' not in v)]
            if len(set(ingredient_keys)) > 0:
                ing_skipped_recipes += 1
                continue

            f.write(json.dumps(recipe))
            f.write("\n")
            # Skipping recipes with 'unknown' tokens in there ingredients
            unknown_tokens = [i['unknown'] for i in recipe['tokenized_ingredients']
                              if len(i['unknown']) > 0]

            len1_unknown_tokens = [i for i in unknown_tokens if len(i) == 1]
            if len(len1_unknown_tokens) == 1:
                unknown_tokens = list(
                    set(unknown_tokens) - set(len1_unknown_tokens))

            # Removing stop words
            old_unknown_tokens = list(unknown_tokens)
            old_len = len(unknown_tokens)
            unknown_tokens = [i for i in unknown_tokens if i not in stop_words]

            '''
            tom_unk = [i for i in unknown_tokens if 'tomato' in i or 'potato' in i]
            if len(tom_unk) > 0:
                _print_array(recipe)
                import pdb; pdb.set_trace()
            '''

            if len(unknown_tokens) > 0:
                for i in unknown_tokens:
                    unknown_formats[i] += 1
                unk_skipped_recipes += 1
                continue

            ingredients = [xencode(i["ingredient"]).strip() for i in
                           recipe['tokenized_ingredients'] if i.get("ingredient", None)]
            if len(ingredients) > 0:
                extracted_data.append(
                    {
                        "recipe": recipe["name"],
                        "ingredients": list(set(ingredients))
                    }
                )
            if (len(extracted_data) > 0 and len(extracted_data) % 1000 == 0) \
                    or records_to_parse == 0:
                msg = "Total Records: %s, Records to Parse: %s, Extracted Records: %s"
                logger.info(
                    msg, total_records_count, records_to_parse, len(
                        extracted_data)
                )

    db.client.close()
    '''
    if extracted_data:
        msg = "Total Records: %s, Extracted Records: %s, Total Skipped Recipes: %s, "
        msg += "Alchol Recipes: %s, Incorrect Recipes: %s, Unknown Recipes: %s"
        logger.info(
            msg, total_records_count, len(extracted_data),
            catg_skipped_recipes + ing_skipped_recipes + unk_skipped_recipes,
            catg_skipped_recipes, ing_skipped_recipes, unk_skipped_recipes
        )
        outfile = os.path.join(DATADIR, 'IngredientsCustom.json')
        with open(outfile, 'w') as f:
            f.write(json.dumps(extracted_data))

    if unknown_formats:
        unknown_formats = sorted(
            unknown_formats.items(),
            key=lambda x: x[-1],
            reverse=True
        )
        logger.info(
            "Len of unique Unknown Tokens: %s",
            len(unknown_formats)
        )
        outfile = os.path.join(DATADIR, 'UnknownFormats.json')
        with open(outfile, 'w') as f:
            f.write(json.dumps(unknown_formats))
    '''
    logger.info("Time taken for script to complete: %s",
                str(datetime.now() - start_time))


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-c", "--collection-name", dest="collection_name",
                      default="cleansed_ingredients",
                      help="Collection name from which cvs should be generated"
                      )
    (options, args) = parser.parse_args()
    collection_to_json(options.collection_name)

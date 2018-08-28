import os
import json
from datetime import datetime
from optparse import OptionParser

import pandas as pd
import pymongo

from utils import xencode, get_logger

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
DATADIR = os.path.join(BASEDIR, 'data')
if not os.path.isdir(DATADIR):
    os.mkdir(DATADIR)

logger = get_logger('generate_json.log')


def get_master_mongo_conn():
    """Gets mongo db connection to chefd-ingredients-research"""
    MONGO_URI = 'mongodb://saran:Saran1!@ds141165-a0.mlab.com:41165/chefd-ingredients-research'
    client = pymongo.MongoClient(MONGO_URI)
    db = client.get_default_database()
    return db


def collection_to_csv(collection_name):
    start_time = datetime.now()
    logger.info(
        "Started process, Starttime: %s",
        str(start_time),
    )
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
    for recipe in recipes:
        ingredients = [xencode(i["ingredient"]).strip() for i in
                       recipe['tokenized_ingredients'] if i.get("ingredient", None)]
        if len(ingredients) > 0:
            extracted_data.append(
                {
                    "recipe": recipe["name"],
                    "ingredients": list(set(ingredients))
                }
            )
        records_to_parse -= 1
        if (len(extracted_data) > 0 and len(extracted_data) % 1000 == 0) \
                or records_to_parse == 0:
            msg = "Total Records: %s, Records Parsed: %s, Extracted Records: %s"
            logger.info(
                msg, total_records_count, records_to_parse, len(extracted_data)
            )

    db.client.close()
    if extracted_data:
        outfile = os.path.join(DATADIR, 'Ingredients.json')
        with open(outfile, 'w') as f:
            f.write(json.dumps(extracted_data))

    logger.info("Time taken for script to complete: %s",
                str(datetime.now() - start_time))


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-c", "--collection-name", dest="collection_name",
                      default="cleansed_ingredients",
                      help="Collection name from which cvs should be generated"
                      )
    (options, args) = parser.parse_args()
    collection_to_csv(options.collection_name)

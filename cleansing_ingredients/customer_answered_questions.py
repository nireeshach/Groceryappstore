import os
import json
import re
from copy import deepcopy
from datetime import datetime
from optparse import OptionParser

import pandas as pd
import pymongo

from utils import get_logger

BASEDIR = os.path.dirname(os.path.realpath(__file__))
DATADIR = os.path.join(BASEDIR, 'data')
if not os.path.isdir(DATADIR):
    os.mkdir(DATADIR)

logger = get_logger("customer_answered_questions.log")


def get_master_mongo_conn(env='dev'):
    """Gets mongo db connection"""
    if env == 'prod':
        MONGO_URI = 'mongodb://chefd_prod:chefd_prod@ds135459-a0.mlab.com:35459/'
        MONGO_URI += 'prod_chefd_1_gb?replicaSet=rs-ds135459'
    else:
        MONGO_URI = 'mongodb://staging_chefd:staging_chefd@ds141098.mlab.com:41098/'
        MONGO_URI += 'staging-chefd-replica'
    client = pymongo.MongoClient(MONGO_URI)
    db = client.get_default_database()
    return db


def collection_to_csv(env='prod'):
    start_time = datetime.now()

    logger.info(
        "Started process, Starttime: %s",
        str(start_time),
    )

    env = env.lower()
    customer_pref_collection = 'customer_preference'
    pref_ques_collection = 'preference_question'
    db = get_master_mongo_conn(env=env)

    customer_emails = {}
    for record in db.customer.find({"email": {"$exists": True}}):
        customer_id = int(record.get('customer_id', ''))
        if not customer_id:
            continue
        customer_emails[customer_id] = record.get('email', '')

    question_ids = set()
    for record in db.question_set.find({'set_id': {'$in': [1, 2]}}):
        for qid in record['questions']:
            question_ids.add(qid)

    prefq_answers_dict = {}
    default_data = {}
    all_questions = set()
    for record in db[pref_ques_collection].find(
            {"question_id": {"$in": list(question_ids)}}, no_cursor_timeout=True):
        p_dict = prefq_answers_dict.setdefault(record['question_id'], {})
        p_dict['text'] = record['text']
        for answer in record['answers']:
            p_dict[answer['answer_id']] = answer['text']
            qkey = record['text'] + ': ' + answer['text']
            default_data[qkey] = 0
            all_questions.add((record['question_id'], qkey))

    headers = [i[-1] for i in sorted(all_questions)]
    headers.insert(0, 'customer_id')
    headers.insert(1, 'customer_email')
    if len(question_ids) > 0:
        query = {"question_id": {"$in": list(question_ids)}}
    else:
        query = {}
    records = db[customer_pref_collection].find(
        query,
        no_cursor_timeout=True
    )

    total_records_count = records.count()
    parsed_records = 0
    extracted_data = {}
    for record in records:
        customer_id = int(record['customer_id'])
        question_id = int(record['question_id'])
        answerd_questions = record['answers']
        prefq_data = prefq_answers_dict.get(question_id, {})
        if not prefq_data:
            logger.info(
                "Failed to get Preference Question data for question_id: %s", question_id)
            continue

        data = extracted_data.setdefault(customer_id, {})
        if not data:
            data.update(default_data)
            data['customer_id'] = customer_id

        data['customer_email'] = customer_emails.get(customer_id, '')
        question = prefq_data['text']
        for aid in answerd_questions:
            try:
                answer = prefq_data[aid]
            except KeyError:
                continue
            qkey = question+': '+answer
            data[qkey] = headers.index(qkey) - 1

    db.client.close()
    file_name = os.path.join(DATADIR, 'preferenceQuestions.csv')
    df = pd.DataFrame(extracted_data.values(), columns=headers)
    df.to_csv(file_name, sep='\t', encoding='utf-8', index=False)
    logger.info("Time taken for script to complete: %s",
                str(datetime.now() - start_time))


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option(
        "-e", "--env", dest="env",
        default="prod",
        help="Env to load data from, Ex: prod or dev. Default prod"
    )

    (options, args) = parser.parse_args()
    collection_to_csv(env=options.env)

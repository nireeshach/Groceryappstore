"""
Extracts common words(ie word before and after ingredient text)
"""

import os
import re
import json
import sys
from copy import deepcopy
from collections import defaultdict
from datetime import datetime
from itertools import groupby
from operator import itemgetter
from optparse import OptionParser

import pandas as pd
from unidecode import unidecode

from utils import get_master_mongo_conn, \
    get_ing_mongo_conn, \
    standardize_ingredient, \
    get_ingredientmaster_values, \
    get_logger

BASEDIR = os.path.dirname(os.path.realpath(__file__))
DATADIR = os.path.join(BASEDIR, 'data')
if not os.path.isdir(DATADIR):
    os.mkdir(DATADIR)

logger = get_logger('common_words.log')


class CommonWords(object):
    """Tokenizes ingredient text"""

    def __init__(self, options):
        """
        Creating mongo connection and calling main method
        diing_datatly from initialization method
        """
        self.start_time = datetime.now()
        self.collection_name = options.collection_name
        self.ing_collection = options.ing_collection
        self.testrun = options.testrun
        self.ingredients_file = options.ingredients_file
        self.threshold_limit = int(options.threshold_limit)
        self.master_db = get_master_mongo_conn()
        self.ing_db = get_ing_mongo_conn()
        self.before_char_re = r'[\s,\-:\(\d\*\/x]'
        self.after_char_re = r'[\s,\-:\)\*\/]'
        # To match '2 1/2' or  '1/2' or '2.4' or '2' strings
        self.numbers_re = r'\d+\s\d+\/\d+|\d+\/\d+|\d+\.?\d+|\d+'
        self.start_process()

    def merge_two_dicts(self, x, y):
        z = x.copy()   # start with x's keys and values
        z.update(y)    # modifies z with y's keys and values & returns None
        return z

    def basic_cleaning(self, ingredient):
        """
        Does basic cleansing provided a text

        Returns:
            Cleaned text
        """
        # Removing extra spaces from ingredient text
        ingredient = re.sub(' +', ' ', ingredient).strip()

        # Removing Special characters from end and start of ing text
        sp_re_pattern = r'(\*+|:+|,+|-+|\(|\))'
        sp_re = re.compile(sp_re_pattern, re.IGNORECASE)
        for _v in sp_re.findall(ingredient):
            ingredient = ingredient.replace(_v, ' %s ' % _v)

        # Removing extra spaces from ingredient text
        ingredient = re.sub(' +', ' ', ingredient).strip()

        return ingredient

    def extract_neighbor_words(self, cleaned_ing_text, before_words, after_words):
        number_of_words = {
            1: r'\w+',
            2: r'\w+[\s,\-:\)\(\*\/]\w+',
            # '3': r'\w+.*?\w+.*?\w+',
            # '4': r'\w+.*?\w+.*?\w+.*?\w+',
        }
        match_patterns = {
            "after": r'{}[\s,\-:\)\(\*\/]({})',  # pattern to match after words
            # pattern to match before words
            "before": r'({})[\s,\-:\)\(\*\/]{}'
        }
        ignore_words = ["for", "or", "to"]
        cleaned_ing_text = ' '+cleaned_ing_text+' '
        for ing_match in self.ing_match_re.findall(cleaned_ing_text):
            ing_match = ing_match.strip()
            for position, match_pattern in match_patterns.items():
                for word_count, _pattern in number_of_words.items():
                    if position == 'after':
                        common_word_re = re.compile(
                            match_pattern.format(
                                ing_match,
                                _pattern
                            ),
                            re.IGNORECASE
                        )
                    else:
                        common_word_re = re.compile(
                            match_pattern.format(
                                _pattern,
                                ing_match
                            ),
                            re.IGNORECASE
                        )
                    for matched_word in common_word_re.findall(
                            cleaned_ing_text.strip()):
                        if not matched_word or \
                                matched_word.isdigit() or \
                                len(matched_word.split(' ')) != word_count:
                            continue
                        has_ignore_words = [True for i in ignore_words
                                            if i in matched_word]
                        if len(has_ignore_words) > 0:
                            continue
                        if position == 'after':
                            check_word = matched_word.split(' ')[0]
                        else:
                            check_word = matched_word.split(' ')[-1]
                        if matched_word in self.ing_master_values or \
                                check_word in self.ing_master_values:
                            # If matched_word/check_word is in Ing master values
                            # or check_word present in ignore_words
                            # stopping the iteration, with out trying for the next words
                            continue

                        if position == 'after':
                            _words = after_words.setdefault(word_count, {})
                        else:
                            _words = before_words.setdefault(word_count, {})
                        val_dict = _words.setdefault(
                            ing_match, {}
                        )
                        if position == 'after':
                            key = ing_match + ' ' + matched_word.strip()
                        else:
                            key = matched_word.strip() + ' ' + ing_match
                        val_data = val_dict.setdefault(key, {})
                        val_data['value'] = val_data.get('value', 0) + 1
                        val_data.setdefault('actual_ingredient', []).append(
                            cleaned_ing_text.strip()
                        )

    def start_process(self):
        """
        This is the main function where the common words extraction starts
        """
        logger.info(
            "Started process, Starttime: %s, IsTestrun: %s",
            str(self.start_time), self.testrun
        )

        db_start_time = datetime.now()

        self.ing_master_values = []
        ing_data = get_ingredientmaster_values(
            self.ing_db,
        )
        self.replace_strings, ingmaster_values, \
            ingredient_dict, alcoholic_beverages, \
            nonfood_goods, ingredient_patterns, valid_skus = ing_data
        self.ing_master_values.extend(ingmaster_values.keys())
        self.ing_master_values.extend(alcoholic_beverages)
        self.ing_master_values.extend(nonfood_goods)

        if self.ingredients_file and os.path.isfile(self.ingredients_file):
            self.testrun = True
            df = pd.read_csv(self.ingredients_file, sep='\t', header=0)
            ingredients = df['Ingredients'].tolist()
            total_ing_count = len(ingredients)
        elif self.testrun == True:
            ingredients = [
                'almond',
                'apple',
                'lemon',
                'almond butter',
                'apple cinder',
                'cayenne pepper',
                'all purpose flour'
            ]
            total_ing_count = len(ingredients)
        else:
            ingredients = ingredient_dict.keys()
            total_ing_count = len(ingredients)
        logger.info(
            "Time taken to get Ingredients: %s, TotalRecord: %s",
            str(datetime.now() - db_start_time)[:-3], total_ing_count
        )

        self.ing_values = sorted(
            list(ingredients),
            key=lambda x: len(x),
        )
        match_string = "|".join(self.ing_values)
        self.ing_match_re = re.compile(r'(?<={})({})(?={})'.format(self.before_char_re, match_string,
                                                                   self.after_char_re), re.IGNORECASE)

        parsed_records = 0
        after_words = {}
        before_words = {}
        limit = None
        total_recipes_count = self.master_db[self.collection_name].count()
        if self.testrun == True:
            total_recipes_count = 1000

        records_to_parse = total_recipes_count
        for recipe in self.master_db[self.collection_name].find(
            {},
            {"tokenized_ingredients.actual_ingredient": 1, "_id": 0},
            no_cursor_timeout=True
        ).limit(total_recipes_count):
            for ing_data in recipe['tokenized_ingredients']:
                cleaned_ing_text, tokens_replaced = standardize_ingredient(
                    ing_data['actual_ingredient'],
                    replace_strings=self.replace_strings,
                    before_char_re=self.before_char_re,
                    after_char_re=self.after_char_re,
                    cleaning_func=self.basic_cleaning
                )
                cleaned_ing_text = cleaned_ing_text.lower()
                self.extract_neighbor_words(
                    cleaned_ing_text, before_words, after_words
                )

            records_to_parse -= 1
            parsed_records += 1
            if (parsed_records % 1000 == 0 or records_to_parse == 0):
                msg = "Total Records: %s, Parsed %s records, "
                msg += "Records to Prase: %s, TimeLapsed: %s"
                logger.info(
                    msg, total_recipes_count, parsed_records, records_to_parse,
                    str(datetime.now() - self.start_time)[:-3]
                )

        for pattern, data in {'after': after_words, 'before': before_words}.items():
            for _words, _data in data.items():
                final_extracted_data = []
                for ing_val, words_dict in _data.items():
                    words_dict = dict(
                        (k, v) for k, v in words_dict.items()
                        if v['value'] > self.threshold_limit
                    )
                    if len(words_dict) > 0:
                        values = [self.merge_two_dicts({'combination': k}, v)
                                  for k, v in words_dict.items()]
                        final_extracted_data.append(
                            {'ingredient': ing_val, 'combinations': values}
                        )

                if len(final_extracted_data) > 0:
                    file_name = os.path.join(
                        DATADIR,
                        'commonWords_%s_%swords.json' % (pattern, _words)
                    )
                    logger.info(
                        "Number of Ingredients in %s file: %s",
                        file_name, len(final_extracted_data)
                    )
                    with open(file_name, 'w') as f:
                        f.write(json.dumps(final_extracted_data))
        logger.info("Time taken for script to complete: %s",
                    str(datetime.now() - self.start_time))


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option(
        "-t", "--testrun", dest="testrun",
        action="store_true", default=False,
        help="If you want to testrun just pass -t option"
    )
    parser.add_option(
        "-c", "--collection-name", dest="collection_name",
        default="cleansed_ingredients",
        help="Collection name from which Actual Ingredients texts should be picked"
    )
    parser.add_option(
        "-i", "--ing-collections", dest="ing_collection",
        default="ingredient",
        help="Collection name from which Ingredients should be picked"
    )
    parser.add_option(
        "-f", "--ingredients-file", dest="ingredients_file",
        metavar="FILE",
        default="",
        help="Ingredients file"
    )
    parser.add_option(
        "-l", "--threshold", dest="threshold_limit",
        metavar="FILE",
        default="30",
        help="Ingredients file"
    )

    (options, args) = parser.parse_args()
    CommonWords(options)

"""
Tokenizes actual ingredient text, using ingredientMaster db collections
"""

import os
import re
import json
import argparse
from copy import deepcopy
from collections import OrderedDict
from datetime import datetime
from itertools import groupby
from operator import itemgetter

from unidecode import unidecode

from converters import OunceConverter, find_chefd_category
from utils import get_master_mongo_conn, \
    get_ing_mongo_conn, \
    xencode, _print_array, \
    get_ingredientmaster_values, \
    standardize_ingredient, \
    convert_to_float, convert_datatype, \
    get_conversions_data, \
    get_logger

BASEDIR = os.path.dirname(os.path.realpath(__file__))
DATADIR = os.path.join(BASEDIR, 'data')
if not os.path.isdir(DATADIR):
    os.mkdir(DATADIR)

logger = get_logger('tokenize_ingredients.log')


class TokenizeIngredients(object):
    """Tokenizes ingredient text"""

    def __init__(self, cmd_options):
        """
        Creating mongo connection and calling main method
        directly from initialization method
        """
        self.start_time = datetime.now()
        self.get_defaults(cmd_options)
        self.clean_ingredients()

    def get_defaults(self, cmd_options):
        self.source = cmd_options.source
        self.collection_name = cmd_options.collection_name
        self.cleansed_col_name = cmd_options.cleansed_collection_name
        self.testrun = cmd_options.testrun
        self.test_ingredients_file = cmd_options.test_file
        self.master_db = get_master_mongo_conn()
        self.ing_db = get_ing_mongo_conn()
        self.before_char_re = r'[\s,:\(\d\*\/x\.\;\)]'
        self.after_char_re = r'[\s,:\)\*\/\.\;\(]'
        # Re to match Decimal, fraction, Whole and Mixed numbers
        # Ex '2.4', '1/2', '2', '2 1/2', '2%'
        self.numbers_re = r'\d+\s\d+\/\d+|\d+\/\d+|\d+\.?\d+|\d+\.?\d+%|\d+%|\d+'
        self.ounce_converter = OunceConverter()
        self.convert_states = ["liquid", "small solids", "ground"]

    def basic_cleaning(self, ingredient, replace_special_chars=False,
                       remove_special_chars=False):
        """
        Does basic cleansing provided a text

        Returns:
            Cleaned text
        """
        # Removing extra spaces from ingredient text
        ingredient = re.sub(' +', ' ', ingredient).strip()

        if replace_special_chars:
            # Replacing special characters with a space before and after
            sp_re_pattern = r'(\*+|:+|,+|\(|\))'
            sp_re = re.compile(sp_re_pattern, re.IGNORECASE)
            for _v in sp_re.findall(ingredient):
                ingredient = ingredient.replace(_v, ' %s ' % _v)

            # Adding space around hyphen if there is a number before or after hyphen
            hyphen_re = re.compile(
                r'(?<=\d)(-)(?=[a-z])|([a-z])(-)(?=\d)', re.IGNORECASE)
            if hyphen_re.search(ingredient):
                ingredient = hyphen_re.sub(' - ', ingredient)

            # Removing extra spaces from ingredient text
            ingredient = re.sub(' +', ' ', ingredient).strip()

        if remove_special_chars:
            # Removing Special characters from end and start of ing text
            end_re_pattern = r'(\*+)$|(:+)$|(,+)$|(\.+)$|(\-+)$'
            start_re_pattern = r'^(\*+)|^(:+)|^(,+)|^(\.+)|^(\-+)'
            sp_re = re.compile(end_re_pattern + '|' +
                               start_re_pattern, re.IGNORECASE)
            while sp_re.search(ingredient):
                ingredient = sp_re.sub('', ingredient).strip()
            # Removing extra spaces from ingredient text
            ingredient = re.sub(' +', ' ', ingredient).strip()

        return ingredient

    def get_continous_numbers(self, numbers_lst, multi=False):
        """
        Given a list of numbers returns list of lists with continuous numbers

        Returns:
            Lists of lists with continous numbers
            ie: [[1,2,3], [5,6,7], ..]
        """
        continous_numbers_lst = []

        for grouped_data in groupby(enumerate(numbers_lst), lambda x: x[0]-x[1]):
            _value = map(itemgetter(1), grouped_data[1])
            _value = [i for i in _value]
            if multi and len(_value) <= 1:
                continue

            continous_numbers_lst.append(_value)

        return continous_numbers_lst

    def combine_ingredient_text(self, ing_tokens, token_types):
        """
        Groups continuous unknown token types as one
        ie  ["value", "uom", "", "", "", "part", "", "", "form"] to
            ["value", "uom", "", "part", "", "form"]

        Returns:
            Cleaned ing_tokens and token_types
        """
        ungrouped_tokens_index = [
            i for i, v in enumerate(token_types) if not v]

        # Getting continuos index number for combining text
        continous_indexes = []
        for _indexes in self.get_continous_numbers(ungrouped_tokens_index):
            if len(_indexes) > 1:
                continous_indexes.append(_indexes)
            else:
                token_types[_indexes[0]] = 'unknown'

        if not continous_indexes:
            return ing_tokens, token_types

        remove_indexes = []
        for index_list in continous_indexes:
            start_index = index_list[0]
            combined_val = " ".join(ing_tokens[i] for i in index_list)
            ing_tokens[start_index] = combined_val.replace(' - ', '-').strip()
            token_types[start_index] = 'unknown'
            remove_indexes.extend(index_list[1:])

        for index in sorted(remove_indexes, reverse=True):
            ing_tokens.pop(index)
            token_types.pop(index)

        return ing_tokens, token_types

    def tokenize_matched_values(self, match_value, ing_tokens, token_types):
        """
        Tokenizes the matched property from the ingredient text

        Returns:
            Cleaned ing_tokens(lst) and token_types(lst)
        """
        try:
            value_type = self.ingmaster_values[match_value]
        except:
            value_type = None
            special_keys = [k for k in self.ingmaster_values if '.*' in k]
            for special_key in special_keys:
                if re.search(special_key, match_value):
                    value_type = self.ingmaster_values[special_key]
            if not value_type:
                return ing_tokens, token_types

        ing_tokens, token_types = self.clean_matched_tokens(
            match_value, value_type, ing_tokens, token_types
        )
        return ing_tokens, token_types

    def clean_tokens(self, ing_tokens, token_types, values):
        """
        Removed unwanted text from tokens used for extractions

        Returns:
            Cleaned ing_tokens and token_types
        """
        for i, v in enumerate(ing_tokens):
            if v == '(':
                token_types[i] = 'start_parenthesis'
            elif v == ')':
                token_types[i] = 'end_parenthesis'
            elif v == ',':
                token_types[i] = 'comma'
            elif v == 'or':
                token_types[i] = 'or'
            elif v == '-' or v == 'and':
                is_before_token = True
                if (i - 1) >= 0 and token_types[i - 1] == '':
                    is_before_token = False

                try:
                    if token_types[i + 1] == '':
                        is_after_token = False
                    else:
                        is_after_token = True
                except IndexError:
                    is_after_token = True

                if is_before_token and is_after_token:
                    if v == 'and':
                        token_types[i] = 'ignore'
                    else:
                        token_types[i] = 'hyphen'
            elif '##value##' in v or '##parsed##' in v:
                v = v.replace('##parsed##', '').strip()
                if '##value##' in v:
                    for _v in re.findall(r'##value##\d+', v):
                        val_index = int(_v.replace('##value##', '').strip())
                        v = v.replace(_v, values[val_index])
                    if token_types[i] == '':
                        token_types[i] = 'value'
                ing_tokens[i] = v
        return ing_tokens, token_types

    def convert_plural_tokens(self, ingredient, ing_tokens, tokens_replaced):
        """
        """
        re_plural = re.compile(r's$', re.IGNORECASE)
        plural_tokens = [(i, v) for i, v in enumerate(ing_tokens)
                         if re_plural.search(v)]
        for token_index, plural_token in plural_tokens:
            # Removing plurals by removing 's' if the subtext exists in ingredients
            converted_token = re_plural.sub('', plural_token).strip()
            previous_tokens = ing_tokens[:token_index+1]
            previous_tokens_len = len(previous_tokens)
            prev_tokens_combinations = [" ".join(previous_tokens[previous_tokens_len-i:])
                                        for i in range(2, previous_tokens_len+1)]

            if any([i in self.ing_values or i in self.ingmaster_vals
                    or i in self.replace_strings for i in prev_tokens_combinations]):
                # Not converting as there is an entry with combination of words before
                # plural token in self.ing_values or self.ing_values
                # or self.replace_strings
                return ingredient, ing_tokens

            if converted_token in self.ing_values or \
                    converted_token in self.ingmaster_vals or \
                    converted_token in self.replace_strings:
                tokens_replaced[converted_token] = plural_token
                ing_tokens[token_index] = converted_token
                ingredient = ingredient.replace(plural_token, converted_token)
        return ingredient, ing_tokens

    def convert_plurals(self, ingredient, tokens_replaced):
        """
        """
        re_plural = re.compile(r's$', re.IGNORECASE)
        ing_tokens = ingredient.split(" ")
        plural_tokens = [(i, v) for i, v in enumerate(ing_tokens)
                         if re_plural.search(v)]
        for token_index, plural_token in plural_tokens:
            # Removing plurals by removing 's' if the subtext exists in ingredients
            converted_token = re_plural.sub('', plural_token).strip()
            previous_tokens = ing_tokens[:token_index+1]
            previous_tokens_len = len(previous_tokens)
            prev_tokens_combinations = [" ".join(previous_tokens[previous_tokens_len-i:])
                                        for i in range(2, previous_tokens_len+1)]

            if any([i in self.ing_values or i in self.ingmaster_vals
                    or i in self.replace_strings for i in prev_tokens_combinations]):
                # Not converting as there is an entry with combination of words before
                # plural token in self.ing_values or self.ing_values
                # or self.replace_strings
                return ingredient

            if converted_token in self.ing_values or \
                    converted_token in self.ingmaster_vals or \
                    converted_token in self.replace_strings:
                tokens_replaced[converted_token] = plural_token
                ingredient = ingredient.replace(plural_token, converted_token)
        return ingredient

    def check_special_charc(self, match_value, value_type, _index,
                            ing_tokens, token_types):
        """
        This function checks for the special charcter in macthed value.
        If exists, adds it as a new token
        """
        new_value = ing_tokens[_index]
        if new_value != match_value:
            special_char = new_value.replace(match_value, '').strip()
            if new_value.startswith(special_char):
                ing_tokens.insert(_index, special_char)
                token_types.insert(_index, '')
                # Incrementing _index value as special charcter is inserted in _index
                # and actual value is moved to _inxed + 1 postion
                _index += 1
            else:
                ing_tokens.insert(_index + 1, special_char)
                token_types.insert(_index + 1, "")
            ing_tokens[_index] = match_value
        token_types[_index] = value_type
        ing_tokens[_index] = '##parsed##' + ing_tokens[_index]
        return ing_tokens, token_types

    def check_token_matches(self, token, match):
        if len(token) == 1 and (not token.isdigit() and not token.isalpha()):
            # Skipping re characters like ( . etc
            return False

        match_re = re.compile(
            r'^\s?{}({}){}\s?$'.format(
                self.before_char_re,
                match,
                self.after_char_re
            ),
            re.IGNORECASE
        )
        return bool(match_re.search(' '+token+' '))

    def clean_matched_tokens(self, match_value, value_type, ing_tokens,
                             token_types):
        """
        This function cleanes the ing_tokens(combines)
        and populates resp token_type

        Returns:
            Cleaned ing_tokens(lst) and token_types(lst)
        """
        match_value_list = match_value.split()
        if len(match_value_list) > 1:
            is_matched = False
            iter_count = 0
            while not is_matched and iter_count < len(ing_tokens):
                match_val_ind = [i for i, x in enumerate(ing_tokens)
                                 if any(self.check_token_matches(x, thing)
                                        for thing in match_value_list)]
                continous_indexes = self.get_continous_numbers(
                    match_val_ind, multi=True)
                matched_indexes_lst = [i for i in continous_indexes
                                       if len(i) == len(match_value_list)]
                if not matched_indexes_lst:
                    is_matched = True
                    break

                matched_indexes = matched_indexes_lst[0]
                index_val_list = ing_tokens[matched_indexes[0]
                    :matched_indexes[-1]+1]
                start_index = matched_indexes[0]
                end_index = matched_indexes[-1]
                ing_tokens[start_index] = " ".join(index_val_list)
                while end_index > start_index:
                    ing_tokens.pop(end_index)
                    token_types.pop(end_index)
                    end_index -= 1
                ing_tokens, token_types = self.check_special_charc(
                    match_value, value_type,
                    start_index, ing_tokens, token_types
                )
                iter_count += 1
        else:
            try:
                match_val_ind = [i for i, x in enumerate(ing_tokens)
                                 if any(self.check_token_matches(x, thing)
                                        for thing in match_value_list)]
                for start_index in match_val_ind:
                    ing_tokens, token_types = self.check_special_charc(
                        match_value, value_type,
                        start_index, ing_tokens, token_types
                    )
            except ValueError:
                msg = "Value missing, ing_tokens: %s, matched_value: %s"
                logger.error(
                    msg, ing_tokens, match_value_list[0]
                )
        return ing_tokens, token_types

    def tokenize_ingredients(self, ingredient, ing_tokens,
                             token_types, tokens_replaced):
        """
        Tokenizes the matched ingredient from the ingredient text

        Returns:
            Cleaned ingredient text
            Cleaned ing_tokens(lst) and token_types(lst)
        """
        ingredient = ' ' + self.basic_cleaning(ingredient) + ' '
        ing_matches = self.ing_match_re.findall(ingredient)
        for _index, ing_match in enumerate(sorted(
            ing_matches,
            key=lambda x: len(x),
            reverse=True
        )):
            ing_match = ing_match.strip()
            match_re = re.compile(r'(?<={})({})(?={})'.format(
                self.before_char_re,
                ing_match,
                self.after_char_re
            ), re.IGNORECASE)
            if not match_re.findall(ingredient):
                # Skipping as, substring is matched
                continue

            ing_key = '##ingredient##%s' % _index
            ingredient = ingredient.replace(ing_match, ing_key, 1)
            ing_tokens, token_types = self.clean_matched_tokens(
                ing_match, 'ingredient', ing_tokens, token_types
            )

        ingredient = self.basic_cleaning(ingredient)
        return ingredient, ing_tokens, token_types

    def standardize_value(self, cleansed_dict, ingredient_texts):
        uom_indxs = [i for i, v in enumerate(cleansed_dict['tokens'])
                     if v['type'] == 'unit_of_measure']
        for indx, uom_indx in enumerate(uom_indxs):
            try:
                uom = cleansed_dict['tokens'][uom_indx]['standard_token']
                ing = ingredient_texts[indx]
                # Unit of measure is mostly the previous token
                # So taking uom_value index as 1 less than uom index
                uom_value_indx = uom_indx - 1
                value = cleansed_dict['tokens'][uom_value_indx]['standard_token']
                vtype = cleansed_dict['tokens'][uom_value_indx]['type']
                if vtype != 'value':
                    # Getting the previous token if the current token
                    # is not of type "value"
                    uom_value_indx = uom_value_indx - 1
                    if cleansed_dict['tokens'][uom_value_indx]['type'] == 'value':
                        value = cleansed_dict['tokens'][uom_value_indx]['standard_token']
                    else:
                        continue

                if not value:
                    continue

                sku = ing + '_' + uom
                std_values_dict = self.valid_skus.get(sku, {})
                std_values = sorted(std_values_dict.keys())
                float_value = convert_to_float(value)
                if not float_value:
                    continue

                if float_value not in std_values:
                    for v in std_values:
                        if float_value < v:
                            v = std_values_dict[v]
                            uom_dict = cleansed_dict['tokens'][uom_value_indx]
                            uom_dict["standard_token"] = v
                            if indx == 0:
                                cleansed_dict['unit_of_measure'] = uom
                                cleansed_dict['unit_of_measure_value'] = v
                            break
            except IndexError:
                continue

        return cleansed_dict

    def get_items_value(self, cleansed_dict):
        if cleansed_dict['unit_of_measure'] in ['item', 'cup']:
            if cleansed_dict['ingredient'] in self.ing_conversion:
                prep, form, size = '', '', 'medium'
                preps = [i['standard_token'] for i in cleansed_dict['tokens']
                         if i['type'] == 'preparation']
                if preps:
                    prep = preps[0]

                forms = [i['standard_token'] for i in cleansed_dict['tokens']
                         if i['type'] == 'form']
                if forms:
                    form = forms[0]

                sizes = [i['standard_token'] for i in cleansed_dict['tokens']
                         if i['type'] == 'size']
                if sizes:
                    size = sizes[0]

                prep_dict = self.ing_conversion[cleansed_dict['ingredient']]
                sizes_dict = prep_dict.get(prep, {})
                if not sizes_dict:
                    sizes_dict = prep_dict.get(form, {})

                ing_conv_dict = sizes_dict.get(size, {})
                if ing_conv_dict:
                    cup_per_unit = convert_to_float(
                        ing_conv_dict['cup_per_unit'],
                        digits=3
                    )
                    if cleansed_dict['unit_of_measure'] == 'cup':
                        number_of_cups = convert_to_float(
                            cleansed_dict['unit_of_measure_value'],
                            digits=3
                        )
                        no_items = round(
                            (number_of_cups/cup_per_unit)/0.5) * 0.5
                        cleansed_dict['items'] = convert_datatype(no_items)
                    else:
                        number_of_items = convert_to_float(
                            cleansed_dict['unit_of_measure_value'],
                            digits=3
                        )
                        number_of_cups = round(
                            number_of_items * cup_per_unit, 2)
                        cleansed_dict['items'] = cleansed_dict['unit_of_measure_value']
                        cleansed_dict['unit_of_measure'] = 'cup'
                        cleansed_dict['unit_of_measure_value'] = convert_datatype(
                            number_of_cups)
        return cleansed_dict

    def extract_details(self, ingredient, actual_ingredient, tokens_replaced):
        """
        This function extracts and tokenizes the ingredient text

        Returns:
            cleansed ing dict which will be loaded into mongo
        """
        ingredient_texts = []
        unkown_texts = []
        special_tokens = ["start_parenthesis",
                          "end_parenthesis", "comma", "hyphen"]
        cleansed_dict = {
            'actual_ingredient': actual_ingredient,
            'tokens': [],
            'unit_of_measure': None,
            'unit_of_measure_value': None,
            'ingredient': '',
            'chefd_category': '',
            'is_ingredient': False,
            'two_ingredients': False,
            'items': None,
            'valid_uom': False,

        }

        ing_long = OrderedDict(
            [
                ('size', ''),
                ('unit_type', ''),
                ('type', ''),
                ('ingredient', ''),
                ('part', '')
            ]
        )

        prep_unknown_tokens = ['and', 'or']

        if self.alc_match_re.search(ingredient):
            cleansed_dict['chefd_category'] = "alcohol"

        ingredient = xencode(ingredient).strip()
        # ingredient = ingredient.replace('-', ' ')
        ingredient = ' ' + self.basic_cleaning(ingredient) + ' '
        values = re.findall(self.numbers_re, ingredient)
        for _ind, _value in enumerate(values):
            if re.search(r'\d\d\/\d', _value):
                # To handle cases like 11/2 etc
                # This will add space between 1 and 1/2
                _old_value = _value
                _value_lst = [v for v in _value]
                _value_lst.insert(1, ' ')
                _value = "".join(_value_lst)
                ingredient = ingredient.replace(_old_value, _value, 1)
                values[_ind] = _value
                tokens_replaced[_value] = _old_value

            if re.search(r'\s(%s)' % _value, ingredient):
                ingredient = re.sub(r'\s(%s)' % _value,
                                    ' ##value##%s ' % _ind, ingredient, 1)
            else:
                replace_re = re.compile(
                    r'(?<={})({})(?={})'.format(
                        self.before_char_re,
                        _value, self.after_char_re
                    ),
                    re.IGNORECASE
                )
                ingredient = replace_re.sub(
                    r'##value##%s' % _ind, ingredient, 1)

        ing_tokens = []
        ingredient = self.basic_cleaning(ingredient)
        for ing_token in ingredient.split():
            if not ing_token:
                continue

            copy_ing_token = ing_token
            ing_token = self.basic_cleaning(ing_token)
            if copy_ing_token != ing_token:
                ingredient = ingredient.replace(copy_ing_token, ing_token, 1)

            if '/' in ing_token and not re.search(r'(##value##\d+\/##value##\d+)', ing_token):
                if 'l/##value##' in ing_token:
                    # handeling cases like l/2 teaspoon paprika
                    _value, value_txt = ing_token.split('/')
                    value_index = int(value_txt.replace(
                        '##value##', '').strip())
                    values[value_index] = '1/%s' % values[value_index]
                    ingredient = ingredient.replace(
                        ing_token, ing_token.replace('l/', '1/'))
                    ing_tokens.append(value_txt)
                else:
                    _ing_token = ing_token.strip().replace('/', ' / ')
                    ingredient = ingredient.replace(ing_token, _ing_token, 1)
                    ing_tokens.extend([self.basic_cleaning(i)
                                       for i in _ing_token.split(' ') if i])
            elif re.search(r'x(##value##)', ing_token, re.IGNORECASE):
                # Special cases like 2 (18x13") rimmed baking sheets
                # in this case ing_token will be x13
                _ing_token = ing_token.replace('x', 'x ')
                ingredient = ingredient.replace(ing_token, _ing_token)
                ing_tokens.extend([self.basic_cleaning(i)
                                   for i in _ing_token.split(' ') if i])
            elif ing_token == 'l':
                # handeling cases like l creaml whipping plusl l milk
                values.append('1')
                ing_tokens.append('##value##%s' % (len(values) - 1))
            else:
                ing_tokens.append(ing_token)

        token_types = [''] * len(ing_tokens)
        """
        ingredient, ing_tokens = self.convert_plural_tokens(
            ingredient, ing_tokens, tokens_replaced
        )
        """
        ingredient, ing_tokens, token_types = self.tokenize_ingredients(
            ingredient, ing_tokens, token_types, tokens_replaced
        )
        ingredient = ' ' + self.basic_cleaning(ingredient) + ' '
        matched_values = self.match_re.findall(ingredient)
        for match_value in matched_values:
            match_value = match_value.strip()
            macth_re = re.compile(
                r'(?<={})({})(?={})'.format(
                    self.before_char_re,
                    match_value, self.after_char_re
                ),
                re.IGNORECASE
            )
            if not macth_re.search(ingredient):
                # Skipping as, substring is matched
                logger.info(
                    "Skipping MatchedValue: %s, as subsctring is matched in Ingredinet: %s",
                    match_value, ingredient
                )
                continue

            match_value = match_value.strip()
            ing_tokens, token_types = self.tokenize_matched_values(match_value,
                                                                   ing_tokens, token_types)
            ingredient = ingredient.replace(match_value, ' ', 1)
            ingredient = ' ' + self.basic_cleaning(ingredient) + ' '

        ing_tokens, token_types = self.clean_tokens(
            ing_tokens, token_types, values)
        ing_tokens, token_types = self.combine_ingredient_text(
            ing_tokens, token_types)

        # Handeling mixed scenario or cases like 1 tsp. coarse or sea salt
        if "unknown-or-ingredient" in "-".join(token_types):
            ing_tokens, token_types = self.parse_mixed_ingredients(
                ing_tokens, token_types, tokens_replaced
            )

        for i, _type in enumerate(token_types):
            if _type == 'unit_of_measure':
                if i >= 1:
                    value_index = i - 1
                    if token_types[value_index] in special_tokens:
                        # If the token is special cases checking
                        # for the before token to get the value
                        value_index -= 1

                    if value_index >= 0 and token_types[value_index] == 'value' \
                            and ing_tokens[value_index]:
                        if not cleansed_dict.get('unit_of_measure', None):
                            cleansed_dict['unit_of_measure'] = ing_tokens[i]
                            cleansed_dict['unit_of_measure_value'] = ing_tokens[value_index]
            if _type == 'ingredient':
                if ing_tokens[i]:
                    ingredient_texts.append(ing_tokens[i])
            elif _type in ing_long.keys() and not ing_long.get(_type, None):
                ing_long[_type] = ing_tokens[i]
            elif _type == 'unknown':
                unknown_val = self.basic_cleaning(
                    ing_tokens[i], remove_special_chars=True)
                if unknown_val in prep_unknown_tokens:
                    try:
                        bef_type = token_types[i-1]
                    except IndexError:
                        bef_type = ''

                    try:
                        aft_type = token_types[i+1]
                    except IndexError:
                        aft_type = ''
                    if bef_type != 'preparation' and aft_type != 'preparation':
                        unkown_texts.append(unknown_val)
                elif unknown_val:
                    unkown_texts.append(unknown_val)

        if not cleansed_dict['unit_of_measure']:
            values_lst = [(ing_tokens[i], i) for i, v in enumerate(token_types)
                          if v == 'value']
            got_value = False
            for value_lst in values_lst:
                if got_value:
                    break

                unit_value, _index = value_lst
                is_item = False
                next_type = "-".join(token_types[_index+1:_index+3])
                if next_type in ['size-ingredient', 'state-ingredient', 'form-ingredient']:
                    is_item = True
                elif len(token_types) > _index+1 and token_types[_index+1] == 'ingredient':
                    is_item = True
                if is_item:
                    cleansed_dict['unit_of_measure'] = 'item'
                    cleansed_dict['unit_of_measure_value'] = unit_value
                    got_value = True
                elif len(values_lst) == 1 and next_type.startswith('container'):
                    cleansed_dict['unit_of_measure'] = ing_tokens[_index+1]
                    cleansed_dict['unit_of_measure_value'] = unit_value
                    got_value = True

        is_or_pattern = False
        if "ingredient-or-ingredient" in "-".join(token_types):
            is_or_pattern = True

        ingredient_long_texts = OrderedDict()
        ingredient_texts = sorted(
            ingredient_texts,
            key=lambda x: len(x),
            reverse=True
        )
        # Considering only top 5 ingredients as of now
        for i, ingredient_text in enumerate(ingredient_texts[:5]):
            ingredient_text = self.basic_cleaning(
                ingredient_text, remove_special_chars=True)
            if i == 0:
                cleansed_dict['ingredient'] = ingredient_text
                ingredient_long_texts['ingredient_long'] = ingredient_text
            else:
                cleansed_dict['ingredient_{}'.format(i + 1)] = ingredient_text
                if is_or_pattern:
                    ingredient_long_texts['ingredient_long_{}'.format(
                        i + 1)] = ingredient_text

        ing_long['ingredient'] = cleansed_dict['ingredient']
        cleansed_dict["unknown"] = "|".join(unkown_texts)

        tokens_extracted = "-".join(token_types)
        if self.is_ingredient_re.search(tokens_extracted):
            cleansed_dict['is_ingredient'] = True

        inlong_check = True
        if self.nonfood_match_re.search(cleansed_dict['actual_ingredient']):
            cleansed_dict['is_ingredient'] = False
            inlong_check = False
        elif len([i for i in token_types if i == 'ingredient']) > 1 \
                and not is_or_pattern:
            cleansed_dict['is_ingredient'] = False
            inlong_check = False

        if inlong_check:
            # ing_long_txt = " ".join(i.strip() for i in ing_long.values() if i)
            # cleansed_dict['ingredient_long'] = ing_long_txt
            cleansed_dict.update(ingredient_long_texts)
            if is_or_pattern:
                cleansed_dict['two_ingredients'] = True
        else:
            cleansed_dict['ingredient_long'] = ""

        for ind, v in enumerate(ing_tokens):
            org_v = v
            for rpd_token, org_token in tokens_replaced.items():
                if rpd_token in v:
                    org_v = v.replace(rpd_token, org_token).replace('\\', '')

            v = v.replace('\\', '')
            tokens_dict = {"token": org_v,
                           "standard_token": v, "type": token_types[ind]}
            cleansed_dict['tokens'].append(tokens_dict)

        cleansed_dict = self.standardize_value(cleansed_dict, ingredient_texts)
        cleansed_dict = self.get_items_value(cleansed_dict)
        ing_dict = self.ingredient_dict.get(cleansed_dict['ingredient'], {})
        cleansed_dict['state'] = ing_dict.get('state', None)

        if cleansed_dict['state'] in self.convert_states:
            ratio, oz_24, oz_64 = self.ounce_converter.to_ounces(
                cleansed_dict['unit_of_measure'],
                cleansed_dict['unit_of_measure_value']
            )
            cleansed_dict['ounces'] = ratio
            cleansed_dict['ounces24ths'] = oz_24
            cleansed_dict['ounces64ths'] = oz_64
        else:
            cleansed_dict['ounces'] = None
            cleansed_dict['ounces24ths'] = None
            cleansed_dict['ounces64ths'] = None

        if cleansed_dict['unit_of_measure'] in \
                self.state_uom_chart.get(cleansed_dict['state'], []):
            cleansed_dict['valid_uom'] = True

        catg = ing_dict.get('category', '')
        if catg:
            cleansed_dict['category'] = catg
        else:
            cleansed_dict['category'] = None

        cleansed_dict['group'] = ing_dict.get('group', None)
        cleansed_dict['notes'] = ing_dict.get('notes', None)
        cleansed_dict['shelflife'] = ing_dict.get('shelflife', None)
        cleansed_dict['finalspec'] = ing_dict.get('finalspec', None)
        if cleansed_dict['finalspec']:
            finalspec_ing = [
                i.strip() for i in cleansed_dict['finalspec'].split(";") if i.strip()]
        else:
            finalspec_ing = None

        try:
            # Using "token" value of ingredient from "tokens" list as original_ingredient
            cleansed_dict['original_ingredient'] = [i["token"] for i in cleansed_dict["tokens"]
                                                    if i["standard_token"] ==
                                                    cleansed_dict["ingredient"]][0].strip()
        except IndexError:
            cleansed_dict['original_ingredient'] = cleansed_dict['ingredient']

        if finalspec_ing:
            cleansed_dict['ingredient'] = finalspec_ing[0].strip()
        size = []
        for i in cleansed_dict['tokens']:
            if i['type'] == 'size':
                size.append(i['standard_token'])
        cleansed_dict['size'] = ";".join(size)
        if len(size) == 0:
            cleansed_dict['size'] = 'medium'
        _print_array(cleansed_dict)
        import pdb
        pdb.set_trace()

        return cleansed_dict

    def parse_ingredients(self, ingredients):
        """
        Iterates through all the ingredient texts
        and calls other methods for tokenizing

        Returns:
            cleansed ing dict which will be loaded into mongo
        """
        if not isinstance(ingredients, list):
            ingredients = [ingredients]

        cleansed_ingredients = []
        basic_cleaning_time = None
        extracting_time = None
        for ingredient in ingredients:
            actual_ingredient = ingredient
            if isinstance(ingredient, bytes):
                ingredient = unidecode(
                    ingredient.decode('utf-8', 'ignore')).strip()
            else:
                ingredient = unidecode(ingredient).strip()

            start_time = datetime.now()
            tokens_replaced = {}

            # Removing special char \xc2\xae
            # which will be replaced to (r) by unidecode
            ingredient = ingredient.replace('(r)', '')
            ingredient = self.basic_cleaning(ingredient)

            # Converting plurals
            ingredient = self.convert_plurals(ingredient, tokens_replaced)

            ingredient, _tokens_replaced = standardize_ingredient(
                ingredient,
                replace_strings=self.replace_strings,
                before_char_re=self.before_char_re,
                after_char_re=self.after_char_re,
                cleaning_func=self.basic_cleaning
            )
            tokens_replaced.update(_tokens_replaced)

            if not basic_cleaning_time:
                basic_cleaning_time = datetime.now() - start_time
            else:
                basic_cleaning_time += datetime.now() - start_time

            start_time = datetime.now()
            ingredient = self.basic_cleaning(ingredient).lower()
            cleansed_dict = self.extract_details(
                ingredient, actual_ingredient, tokens_replaced
            )
            cleansed_ingredients.append(cleansed_dict)
            if not extracting_time:
                extracting_time = datetime.now() - start_time
            else:
                extracting_time += datetime.now() - start_time
        return cleansed_ingredients, basic_cleaning_time, extracting_time

    def get_categories(self, recp_catg, cleansed_ingredients):
        """
        This function gets the correct chefd_category from category data of recipe
        and appends chefd_category from the cleansed_ingredients

        Returns:
            converted_catg: string with categories seperated by "#<>#"
        """
        recp_catgs = [i['chefd_category'] for i in cleansed_ingredients
                      if i.get('chefd_category', None)]

        parsed_catg = "#<>#".join(set(recp_catgs))
        converted_catg = find_chefd_category(recp_catg)
        if converted_catg and parsed_catg:
            converted_catg += "#<>#" + parsed_catg
        elif parsed_catg:
            converted_catg = parsed_catg
        return converted_catg

    def is_two_ingredients(self, cleansed_ingredients):
        """
            Returns two_ingredients boolean value
            based on cleansed_ingredients two_ingredients value

        Returns
            two_ingredients: boolean value
        """
        return any([i.get('two_ingredients', False) for i in cleansed_ingredients])

    def parse_mixed_ingredients(self, ing_tokens, token_types, tokens_replaced):
        """
        This function is to handle special mixed ingredient scenario
        Ex: 1 tsp. coarse or sea salt
            In this example the first string is not an ingredient,
            but the second one after the 'or' is
            The intention is that there are two ingredients here.
            They are 'coarse salt' and 'sea salt',
            this function helps extracting ingredients data of such cases

        Returns:
            Cleaned ing_tokens and token_types
        """
        or_tokens_index = [i for i, v in enumerate(token_types) if v == 'or']
        for or_token_index in or_tokens_index:
            try:
                unknow_index = or_token_index - 1
                ing_index = or_token_index + 1
                if token_types[unknow_index] != 'unknown' or \
                        token_types[ing_index] != 'ingredient':
                    continue
                if len(ing_tokens[unknow_index]) <= 1:
                    continue

                _unknown_val = ing_tokens[unknow_index]
                _ing_val = ing_tokens[ing_index]
                if not _ing_val:
                    continue
                new_unknown_val = _unknown_val + ' ' + \
                    " ".join(_ing_val.split(' ')[1:]).strip()
                if new_unknown_val in self.ingredient_dict:
                    token_types[unknow_index] = 'ingredient'
                    ing_tokens[unknow_index] = new_unknown_val
                    tokens_replaced[new_unknown_val] = _unknown_val
            except IndexError:
                continue
        return ing_tokens, token_types

    def clean_ingredients(self):
        """
        This is the main function where the tokenization starts
        """
        logger.info(
            "Started cleansing process, Starttime: %s, IsTestrun: %s",
            str(self.start_time), self.testrun
        )
        query_params = {}

        ing_time = datetime.now()
        ing_data = get_ingredientmaster_values()
        self.replace_strings, self.ingmaster_values, \
            self.ingredient_dict, self.alcoholic_beverages, \
            self.nonfood_goods, self.ingredient_patterns, \
            self.valid_skus, self.state_uom_chart = ing_data
        logger.info(
            "Time taken to get Ingredients data from db: %s",
            str(datetime.now() - ing_time)[:-3]
        )

        self.is_ingredient_re = re.compile(
            '{}'.format("|".join(self.ingredient_patterns)),
            re.IGNORECASE
        )

        self.ingmaster_vals = sorted(
            self.ingmaster_values.keys(),
            key=lambda x: len(x),
            reverse=True
        )
        matched_str = "|".join(self.ingmaster_vals)
        self.match_re = re.compile(r'(?<={})({})(?={})'.format(
            self.before_char_re, matched_str,
            self.after_char_re
        ), re.IGNORECASE)

        self.ing_values = sorted(
            self.ingredient_dict.keys(),
            key=lambda x: len(x),
            reverse=True
        )
        match_string = "|".join(self.ing_values)
        self.ing_match_re = re.compile(r'(?<={})({})(?={})'.format(
            self.before_char_re, match_string,
            self.after_char_re
        ), re.IGNORECASE)

        alcoholic_values = sorted(
            self.alcoholic_beverages,
            key=lambda x: len(x),
            reverse=True
        )
        match_string = "|".join(alcoholic_values)
        self.alc_match_re = re.compile(
            r'{}'.format(match_string), re.IGNORECASE)

        nonfood_values = sorted(
            self.nonfood_goods,
            key=lambda x: len(x),
            reverse=True
        )
        match_string = "|".join(nonfood_values)
        self.nonfood_match_re = re.compile(
            r'{}'.format(match_string), re.IGNORECASE)

        self.conv_match_re, self.ing_conversion = get_conversions_data(
            self.ing_db)
        if self.source:
            query_params["source"] = self.source.strip()

        recipes = []
        total_records_count = 0
        db_start_time = datetime.now()
        if self.testrun and os.path.isfile(self.test_ingredients_file):
            with open(self.test_ingredients_file) as f:
                recipes = f.read().splitlines()
            total_records_count = len(recipes)
        else:
            recipes = self.master_db[self.collection_name].find(
                query_params,
                no_cursor_timeout=True
            )
            total_records_count = recipes.count()

        logger.info(
            "Time taken to get recipes from db: %s, TotalRecord: %s",
            str(datetime.now() - db_start_time)[:-3], total_records_count
        )

        load_start_time = datetime.now()
        records_to_parse = total_records_count
        deleted_count = 0
        inserted_count = 0
        total_ingredients = 0
        total_ingredients_iter = 0
        ing_stnd_time = None
        ing_ext_time = None
        extracted_data = []
        ignore_fields = [
            'ingredients',
            'cleaned_ingredients',
            'tokenized_ingredients',
            'created_at',
            'updated_at'
        ]
        for recipe in recipes:
            if self.testrun:
                copy_recipe = {}
                total_ingredients_iter += 1
                parsed_data = self.parse_ingredients(recipe)
                cleansed_ingredients, basic_cleaning_time, extracting_time = parsed_data
                copy_recipe['tokenized_ingredients'] = cleansed_ingredients
                copy_recipe['chefd_category'] = self.get_categories(
                    copy_recipe.get('category', ''),
                    cleansed_ingredients
                )
                copy_recipe['two_ingredients'] = self.is_two_ingredients(
                    cleansed_ingredients)
                extracted_data.append(copy_recipe)
            else:
                pstart_time = datetime.now()
                copy_recipe = deepcopy(recipe)
                for ignore_field in ignore_fields:
                    if ignore_field in copy_recipe:
                        copy_recipe.pop(ignore_field)

                ingredients = [i['actual_ingredient']
                               for i in recipe['cleaned_ingredients']]
                logger.info(
                    "Parsing Recipe: %s, Source: %s, Url: %s, IngCounr: %s",
                    recipe['_id'], recipe['source'], recipe['url'], len(
                        ingredients)
                )
                total_ingredients_iter += len(ingredients)
                parsed_data = self.parse_ingredients(ingredients)
                cleansed_ingredients, basic_cleaning_time, extracting_time = parsed_data
                copy_recipe['chefd_category'] = self.get_categories(
                    copy_recipe.get('category', ''),
                    cleansed_ingredients
                )
                copy_recipe['two_ingredients'] = self.is_two_ingredients(
                    cleansed_ingredients)

                if not ing_stnd_time:
                    ing_stnd_time = basic_cleaning_time
                else:
                    ing_stnd_time += basic_cleaning_time

                if not ing_ext_time:
                    ing_ext_time = extracting_time
                else:
                    ing_ext_time += extracting_time
                copy_recipe['tokenized_ingredients'] = cleansed_ingredients
                extracted_data.append(copy_recipe)
                msg = "Completed Parsing Recipe: %s, IngCounr: %s, ParsedCOunt: %s, "
                msg += "ParseTime: %s, StandardizingTime: %s, ExtartionTime: %s"
                logger.info(
                    msg, recipe['_id'], len(ingredients), len(extracted_data),
                    str(datetime.now() - pstart_time)[:-3],
                    str(basic_cleaning_time)[:-3], str(extracting_time)[:-3]
                )

            records_to_parse -= 1
            if (len(extracted_data) == 1000 or records_to_parse == 0) and not self.testrun:
                load_time = datetime.now() - load_start_time
                insert_start_time = datetime.now()
                if deleted_count == 0:
                    delete_time = datetime.now()
                    logger.info("Started Deleting old records")
                    del_obj = self.master_db[self.cleansed_col_name].delete_many({
                    })
                    deleted_count = del_obj.deleted_count
                    logger.info(
                        "Completed Deleting old records. Deleted : %s Records, Timetaken: %s",
                        deleted_count, str(datetime.now() - delete_time)[:-3]
                    )

                ins_obj = self.master_db[self.cleansed_col_name].insert_many(
                    extracted_data)
                inserted_count += len(ins_obj.inserted_ids)
                total_ingredients += total_ingredients_iter
                msg = "BulkInserted %s records. Total Records: %s, Records to Prase: %s, "
                msg += "TotalInsertedCount: %s, ParsedIngredients: %s, ParsingTime: %s, "
                msg += "InsertTime: %s, StandardizingTime: %s, ExtractionTime: %s "
                msg += "TimeLapsed: %s"
                logger.info(
                    msg, len(extracted_data), total_records_count,
                    records_to_parse, inserted_count, total_ingredients_iter,
                    str(load_time)[:-3], str(datetime.now() -
                                             insert_start_time)[:-3],
                    str(ing_stnd_time)[:-3], str(ing_ext_time)[:-3],
                    str(datetime.now() - self.start_time)[:-3]
                )
                extracted_data, total_ingredients_iter = [], 0
                ing_stnd_time, ing_ext_time = None, None
                load_start_time = datetime.now()

        self.master_db.client.close()
        self.ing_db.client.close()
        logger.info(
            "Total Recipes: %s, Total Ingredients: %s, DeletedCount: %s, InsertedCount: %s",
            total_records_count,
            total_ingredients,
            deleted_count,
            inserted_count
        )

        if extracted_data and self.testrun:
            outfile = os.path.join(DATADIR, 'tokenizeIngredients.json')
            with open(outfile, 'w') as f:
                f.write(json.dumps(extracted_data))

        logger.info("Time taken for script to complete: %s",
                    str(datetime.now() - self.start_time))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-t", "--testrun", dest="testrun",
        action="store_true", default=False,
        help="If you want to testrun just pass -t option"
    )
    parser.add_argument(
        "-c", "--collection-name",
        default="ingredientsData",
        help="Collection name from which cvs should be generated"
    )
    parser.add_argument(
        "-m", "--cleansed-collection-name", dest="cleansed_collection_name",
        default="cleansed_ingredients",
        help="Collection name to where the tokenized data should be inserted"
    )
    parser.add_argument(
        "-s", "--source", dest="source",
        default="",
        help="Source name of the data to be cleaned"
    )
    parser.add_argument(
        "-f", "--test-file", dest="test_file",
        metavar="FILE",
        default="test_ingredients_file.txt",
        help="Test Ingredients file"
    )

    args = parser.parse_args()
    TokenizeIngredients(args)

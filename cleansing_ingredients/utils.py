"""
Common methods used in Ingredients research
"""

import logging
import os
import sys
import re
import time
import pymongo
from collections import OrderedDict

BASEDIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(BASEDIR)

import django
from unidecode import unidecode
from fractions import Fraction

# Settingup Django to access Models
PROJECT = os.path.split(BASEDIR)[-1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "%s.settings" % PROJECT)
django.setup()

from masterdata.models import Characteristic, CharacteristicType, \
    Ingredient, State, Category, Group, IngredientConversion


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


def get_ing_mongo_conn():
    """Gets mongo db connection to ingredientmaster"""
    MONGO_URI = 'mongodb://saran:Saran1!@ds113736.mlab.com:13736/ingredientmaster'
    client = pymongo.MongoClient(MONGO_URI)
    db = client.get_default_database()
    return db


def xencode(val):
    """Converts bytes obj to string"""
    if isinstance(val, bytes):
        try:
            val = val.decode('utf-8')
        except UnicodeEncodeError:
            val = val.decode('utf-8', 'ignore')
    return val


def timeit(method):
    """timeit decorator to track method exuction time"""
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms') % \
                (method.__name__, (te - ts) * 1000)
        return result
    return timed


def get_logger(name):
    logfile_dir = os.path.join(BASEDIR, 'logs')
    if not os.path.isdir(logfile_dir):
        os.mkdir(logfile_dir)

    if not name.endswith(".log"):
        name = name.strip() + '.log'

    log_file = os.path.join(logfile_dir, name)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create a file handler
    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)
    return logger


def convert_to_float(value, digits=2):
    """
    Converts int or string of fractions to float
    """

    if isinstance(value, Fraction):
        value = float(round(value, digits))
    else:
        # Removing extra spaces
        value = re.sub(' +', ' ', str(value)).strip()
        try:
            value = float(round(sum(Fraction(s)
                                    for s in value.split()), digits))
        except ValueError:
            value = None
    return value


def convert_datatype(val):
    if isinstance(val, (int, float)):
        val = '{0:g}'.format(val)
    return val


def standardize_ingredient(ingredient,
                           replace_strings='',
                           before_char_re='',
                           after_char_re='',
                           cleaning_func=''):
    """
    Standardizes ingredient text

    Returns:
        Standardized text and replaced values(dict) for reference
    """
    tokens_replaced = {}
    ingredient = cleaning_func(ingredient, replace_special_chars=True)
    ingredient = ' ' + ingredient.strip() + ' '

    # Repalcing strings in replace_strings dict
    stnd_values = sorted(
        replace_strings.keys(),
        key=lambda x: len(x),
        reverse=True
    )

    match_str = "|".join(stnd_values)
    replace_re = re.compile(
        r'(?<={})({})(?={})'.format(
            before_char_re,
            match_str, after_char_re
        ),
        re.IGNORECASE
    )
    for replace_str in replace_re.findall(ingredient):
        replace_str = replace_str.strip()
        stnd_str = replace_strings.get(replace_str, None)
        if not stnd_str:
            stnd_str = replace_strings.get(replace_str.lower(), None)
            if not stnd_str:
                continue

        ingredient = ' ' + ingredient.strip() + ' '
        """
        # To check if standard token already exits in the text
        stnd_re = re.compile(
            r'(?<={})({})(?={})'.format(
                before_char_re,
                stnd_str, after_char_re
            ),
            re.IGNORECASE
        )
        if stnd_re.search(ingredient):
            ingredient = stnd_re.sub('#standtoken#', ingredient)
            ingredient = ' ' + cleaning_func(ingredient) + ' '
        """

        # re to replace strings
        replace_re = re.compile(
            r'(?<={})({})(?={})'.format(
                before_char_re,
                replace_str, after_char_re
            ),
            re.IGNORECASE
        )

        if replace_re.search(ingredient):
            ingredient = replace_re.sub(stnd_str, ingredient)
            tokens_replaced[stnd_str] = replace_str

        #Re substituing standtoken#
        ingredient = ingredient.replace('#standtoken#', stnd_str)
        ingredient = cleaning_func(ingredient)
    return ingredient, tokens_replaced


def get_conversions_data(ing_db):
    """Gets Conversion data from postgresql"""
    ingredients = set()
    ing_conversion = {}
    for record in IngredientConversion.objects.all():
        for ing in record.ingredient.split('; '):
            ing = xencode(ing.strip().lower())
            if not ing:
                continue

            ingredients.add(ing)
            preps = [i for i in record.preparation.split('; ') if i.strip()]
            if not preps:
                preps = ['']

            for prep in preps:
                ing_dict = ing_conversion.setdefault(ing, {})
                ing_prep = ing_dict.setdefault(prep, {})
                ing_size = ing_prep.setdefault(record.size, {})
                ing_size['form'] = record.form
                ing_size['cup_per_unit'] = record.cup_per_unit
                ing_size['category'] = record.category

    ingredients = sorted(
        list(ingredients),
        key=lambda x: len(x),
        reverse=True
    )
    matched_str = "|".join(ingredients)
    match_re = re.compile(r'({})'.format(matched_str), re.IGNORECASE)
    return match_re, ing_conversion


def get_ingredientmaster_values():
    """
    This function gets the data from ingredientMaster collections

    Returns:
        replace_strings(dict): alternative values used for standardizing
            key: alternative value
            value: actual value
        ingmasiter_values(dict): All the collections data excluding the below
            key: collection value
            value: collection_name (for reference)
        ingredient_dict: Values from ingredient collection
            key: ingredient
            value: collection_name (for reference)
        alcoholic_beverages(list): Values from ingredient collection
        nonfood_goods(list): Values from ingredient collection
    """
    ingredient_dict = {}
    replace_strings = {}
    ingmaster_values = {}
    alcoholic_beverages = []
    ingredient_patterns = []
    nonfood_goods = []
    standard_values = []
    state_uom_chart = {}
    valid_skus = {}

    # ingredient dictionary
    for i in Ingredient.objects.all().\
            select_related('category', 'state', 'group'):
        rec_value = i.name
        if isinstance(rec_value, bytes):
            rec_value = unidecode(rec_value.decode('utf-8')).strip().lower()
        else:
            rec_value = unidecode(rec_value).strip().lower()

        # Removing special char \xc2\xae
        # which will be replaced to (r) by unidecode
        rec_value = rec_value.replace('(r)', '')
        # Replacing ( and ) as this will conlfict with re pattern
        rec_value = rec_value.replace('(', r'\(').replace(')', r'\)')
        d = ingredient_dict.setdefault(rec_value, {})
        if i.category:
            d['category'] = i.category.name.lower().strip()
        else:
            d['category'] = None
        if i.state:
            d['state'] = i.state.name.lower().strip()
        else:
            d['state'] = None
        if i.group:
            d['group'] = i.group.name.lower().strip()
        else:
            d['group'] = None

        d['notes'] = i.notes
        d['shelflife'] = i.shelflife
        d['finalspec'] = i.finalspec_str

        if i.alternates_str:
            for oth_form in [i.strip() for i in i.alternates_str.split(';') if i.strip()]:
                if isinstance(oth_form, bytes):
                    oth_form = unidecode(oth_form.decode('utf-8')).strip().lower()
                else:
                    oth_form = unidecode(oth_form).strip().lower()

                # Removing special char \xc2\xae
                # which will be replaced to (r) by unidecode
                oth_form = oth_form.replace('(r)', '')
                # Replacing ( and ) as this will conlfict with re pattern
                oth_form = oth_form.replace('(', r'\(').replace(')', r'\)')
                if oth_form == rec_value:
                    continue
                replace_strings[oth_form] = rec_value

    for rec in Characteristic.objects.all().select_related('type'):
        if not rec.type:
            continue

        if isinstance(rec.name, bytes):
            rec_value = unidecode(rec.name.decode('utf-8')).strip().lower()
        else:
            rec_value = unidecode(rec.name).strip().lower()

        # Removing special char \xc2\xae
        # which will be replaced to (r) by unidecode
        rec_value = rec_value.replace('(r)', '')
        # Replacing ( and ) as this will conlfict with re pattern
        rec_value = rec_value.replace('(', r'\(').replace(')', r'\)')
        if rec.type.name == "alcoholic_beverage":
            alcoholic_beverages.append(rec_value)
        elif rec.type.name == "nonfood_goods":
            nonfood_goods.append(rec_value)
        elif rec.type.name == "standard_values":
            standard_values.append(rec_value)
        elif rec.type.name == "ingredient_patterns":
            ingredient_patterns.append(rec_value)
        elif rec.type.name == "state_uom_chart":
            if rec.additional_info:
                uom_value = eval(rec.additional_info).get(
                    'unit_of_measure', [])
                uom_values = [i.strip()
                              for i in uom_value.split(";") if i.strip()]
                if len(uom_values) > 0:
                    state_uom_chart[rec_value] = uom_values
        elif rec.type.name == "valid_skus":
            i_value = []
            if rec.additional_info:
                i_value = eval(rec.additional_info)
            for value in i_value:
                if isinstance(value, bytes):
                    rec_value = unidecode(
                        value.decode('utf-8')).strip().lower()
                else:
                    rec_value = unidecode(value).strip().lower()

                # Removing special char \xc2\xae
                # which will be replaced to (r) by unidecode
                rec_value = rec_value.replace('(r)', '')
                # Replacing ( and ) as this will conlfict with re pattern
                rec_value = rec_value.replace('(', r'\(').replace(')', r'\)')
                stdv_dict = valid_skus.setdefault(rec.name, {})
                stdv_dict[convert_to_float(rec_value)] = rec_value
        else:
            ingmaster_values[rec_value] = rec.type.name

        if rec.alternates:
            # alternatives dictionary. converting string to list using 'eval'
            for oth_form in eval(rec.alternates):
                if isinstance(oth_form, bytes):
                    oth_form = unidecode(
                        oth_form.decode('utf-8')).strip().lower()
                else:
                    oth_form = unidecode(oth_form).strip().lower()

                # Removing special char \xc2\xae
                # which will be replaced to (r) by unidecode
                oth_form = oth_form.replace('(r)', '')
                # Replacing ( and ) as this will conlfict with re pattern
                oth_form = oth_form.replace('(', r'\(').replace(')', r'\)')
                if oth_form == rec_value:
                    continue
                replace_strings[oth_form] = rec_value

    form_values = []
    prep_values = []
    for k, v in ingmaster_values.items():
        if v == 'form':
            form_values.append(k)
        if v == 'preparation':
            if '<form>' in k:
                prep_values.append(k)

    for j in prep_values:
        prep_dict = dict((j.replace('<form>', i), 'preparation')
                         for i in form_values)
        ingmaster_values.update(prep_dict)

    return replace_strings, ingmaster_values, ingredient_dict, \
        alcoholic_beverages, nonfood_goods, ingredient_patterns, \
        state_uom_chart, valid_skus

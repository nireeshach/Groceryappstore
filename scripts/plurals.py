import os
import re
import sys

BASEDIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(BASEDIR)

import django
import pandas as pd
import numpy as np

# Settingup Django to access Models
PROJECT = os.path.split(BASEDIR)[-1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "%s.settings" % PROJECT)
django.setup()

from masterdata.models import Ingredient

df = pd.read_csv("Plural ingredient - ingredient.csv")
column_values = [''] * len(df)
df['Parent_Ingredient'] = column_values
"""
alternates_dict = dict((a.strip(), i['id']) for i in Ingredient.objects.all().values(
    'id', 'alternates_str') if i['alternates_str']
    for a in i['alternates_str'].split(';'))
"""
for index, row in df.iterrows():
    if row["ingredient"].endswith("s"):
        plural_ingredient = row["ingredient"]
        singular_ingredient = plural_ingredient[:-1]

        try:
            sing_obj = Ingredient.objects.get(name=singular_ingredient)
            if plural_ingredient not in sing_obj.alternates_str:
                plu_obj = Ingredient.objects.get(name=plural_ingredient)
                """
                if plu_obj.isalternate:
                    p_obj = Ingredient.objects.get(
                        id=alternates_dict[plural_ingredient])
                    row["parent_Ingredient"] = p_obj.name
                """
                """
                if sing_obj.isalternate:
                    sing_obj = Ingredient.objects.get(
                        id=alternates_dict[singular_ingredient])
                """
                if plu_obj.alternates_str == sing_obj.name:
                    continue
                if not plu_obj.isalternate:
                    sing_obj.alternates.add(plu_obj)
                    sing_obj.save()
                    row["Parent_Ingredient"] = sing_obj.name
        except Ingredient.DoesNotExist:
            pass
#import pdb
# pdb.set_trace()
df.to_csv('plural_ingredients_updated.csv', index=False)

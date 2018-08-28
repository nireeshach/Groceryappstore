"""uom value conversion to ounces"""

import re

import pymongo

_DECIMAL_PATTERN_ = re.compile(r"^(\d+?\.\d+)")
_FRACTION_PATTERN_ = re.compile(r"^(\d+/\d+)")
_WHOLE_NUMBER_PATTERN_ = re.compile(r"^(\d+)")
_MIXED_NUMBER_PATTERN_ = re.compile(r"^(\d+) (\d+/\d+)")

_CHEFD_CATEGORIES_ = [
    'Appetizer',
    'Breakfast',
    'Brunch',
    'Dessert',
    'Dinner'
    'Liqueur Recipes',
    'Liquor Recipes',
    'Lunch',
    'Main Dish',
    'Side Dish',
    'Snack'
]

_CHEFD_CATEGORIES_LOWER_ = [x.lower() for x in _CHEFD_CATEGORIES_]

_UNITS_OZ = {
    "pinch":  0.0133,
    "dash": 0.0313,
    "splash": 0.0833,
    "teaspoon": 0.1667,
    "tablespoon": 0.5000,
    "ounce": 1.0000,
    "cup": 8.0000,
    "pound": 16.0000,
    "quart": 32.0000,
    "liter": 33.8140,
    "pint": 16.0000,
    "gallon": 128.0000
}


def find_chefd_category(category=''):
    """
    Return a chefd category string if found from
    given category string otherwise None
    """

    if not category:
        return None

    category_lower = category.lower()

    category_finder = ((x, category_lower.find(x))
                       for x in _CHEFD_CATEGORIES_LOWER_
                       if category_lower.find(x) >= 0)

    category_map = sorted(category_finder, key=lambda x: x[1], reverse=True)

    if category_map:
        return category_map[0][0]

    return None


def _parse_fraction(text=''):
    """
    Parse fraction in float
    """
    try:
        parts = text.split('/')
        part1 = int(parts[0])
        part2 = int(parts[1])
        return part1 / float(part2)
    except Exception:
        pass

    return float(0)


def _parse_to_decimal(text=''):
    """
    Parse quantity in float
    """

    try:
        text = (text or '').strip()

        match = _MIXED_NUMBER_PATTERN_.match(text)
        if match:
            return int(match.group(1)) + _parse_fraction(match.group(2))

        match = _FRACTION_PATTERN_.match(text)
        if match:
            return _parse_fraction(match.group(1))

        match = _DECIMAL_PATTERN_.match(text)
        if match:
            return float(match.group(1))

        match = _WHOLE_NUMBER_PATTERN_.match(text)
        if match:
            return float(match.group(1))

        return float(0)

    except Exception:
        pass

    return None


class OunceConverter(object):
    """Uom value to ounce converter"""

    def __init__(self, scale_3rd=24, scale_8th=64):
        self._scale_3rd = scale_3rd
        self._scale_8th = scale_8th

    def to_ounces(self, uom_name, uom_value):
        """Convert given uom value to ounces"""

        uom = (uom_name or "").strip().lower()
        value = _parse_to_decimal(uom_value)
        ratio = _UNITS_OZ.get(uom, 0.000)

        if not uom or not value or not ratio:
            return (None, None, None)

        ounce = ratio * value

        ounce_3rd_dec = ounce * self._scale_3rd
        ounce_8th_dec = ounce * self._scale_8th

        ounce_3rd_int = int(round(ounce_3rd_dec, 0))
        ounce_8th_int = int(round(ounce_8th_dec, 0))

        ounce_3rd_diff = abs(ounce_3rd_dec - ounce_3rd_int)
        ounce_8th_diff = abs(ounce_8th_dec - ounce_8th_int)

        ounce_3rd = None if ounce_3rd_diff else ounce_3rd_int
        ounce_8th = None if ounce_8th_diff else ounce_8th_int

        return(ounce, ounce_3rd, ounce_8th)


_MONGODB_CONNECTION_MASTER_ = "mongodb://chefd-staging:chefd-staging@ds113736.mlab.com:13736/ingredientmaster"
_MONGODB_DATABASE_MASTER_ = "ingredientmaster"


class StandardFormConverter(object):
    """Value Standardize form converter from mongodb"""

    def __init__(self, collection=str, conn_string='', database=''):
        self._collection = collection
        self._connection_string = conn_string or _MONGODB_CONNECTION_MASTER_
        self._database = database or _MONGODB_DATABASE_MASTER_
        self._is_maping_loaded = False
        self._mapped_master = {}

    def _get_mapped_master(self):
        """Load standard forms from mongodb"""

        if self._is_maping_loaded:
            return self._mapped_master

        mongo_client = pymongo.MongoClient(self._connection_string,
                                           connect=False)
        mongo_database = mongo_client.get_database(self._database)
        mongo_collection = mongo_database.get_collection(self._collection)

        mongo_data = mongo_collection.find({}, {
            "_id": 0,
            "alternatives": 1,
            "value": 1
        }, no_cursor_timeout=True)

        mapped_master = {}
        for data in mongo_data:
            std_value = str(data.get("value", "")).lower()
            mapped_master.setdefault(std_value, std_value)

            other_values = data.get("alternatives", [])
            for value in other_values:
                alt_value = str(value).lower()
                mapped_master.setdefault(alt_value, std_value)

        self._is_maping_loaded = True
        self._mapped_master = mapped_master
        return mapped_master

    def standardize(self, value):
        """Get standard form of given value"""

        mappings = self._get_mapped_master()
        return mappings.get(value, value)

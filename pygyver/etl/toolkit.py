""" Analytics Toolkit """
import os
import re
import hashlib
import hmac
from datetime import datetime, timedelta
import logging
import logging.config
import nltk
import pandas as pd
import yaml


class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'


def read_sql_file(path_to_file, **kwargs):
    """ Loads SQL file as a string.
    Arguments:
        path_to_file (string): path to sql file from PROJECT_ROOT
        *kwargs: can be passed if some parameters are to be passed.

    Returns:
        a SQL formated with **kwargs if applicable.

    Example:
        With SQL as:
            "select .. {param2} .. {param1} .. {paramN}"
        *kwargs as:
            param1=value1
            param2=value2
            paranN=valueN
        The functions returns:
            "select .. value2 .. value1 .. valueN"
    """
    full_path = os.path.join(os.environ.get("PROJECT_ROOT"), path_to_file)

    file = open(full_path, 'r')
    sql = file.read()
    file.close()

    if len(kwargs) > 0:
        sql = sql.format_map(SafeDict(**kwargs))
    return sql


def read_yaml_file(path_to_file):
    """ Loads YAML file as a dictionary.
    Arguments:
    - path_to_file (string): path to yaml file from PROJECT_ROOT
    """
    full_path = os.path.join(os.getenv("PROJECT_ROOT"), path_to_file)
    with open(full_path, 'r') as file:
        config = yaml.safe_load(file)
    file.close()
    return config


def customer_hash(email):
    """ Hash Email address using sha256 algorithm with salt.
    Parameters:
    email (string): Email address of the customer
    Returns:
    UID (string)
   """
    if "EMAIL_HASH_SALT" in os.environ:
        pass
    else:
        raise KeyError("EMAIL_HASH_SALT does not exist")
    if isinstance(email, str):
        if email != '':
            email = bytes(email.lower(), 'utf-8')
            salt = bytes(os.environ.get("EMAIL_HASH_SALT"), 'utf-8')
            hash_ = hmac.new(key=salt,
                             digestmod=hashlib.sha256)
            hash_.update(email)
            uid = str(hash_.hexdigest())[0:16]
        else:
            uid = '0000000000000000'
        return uid
    elif email is None:
        uid = '0000000000000000'
        return uid
    else:
        raise KeyError("Email argument should be a string")


def stringify(value):
    """
    Returns the string representation of the value.
    """
    if value is None:
        return 'null'
    elif value is True:
        return 'True'
    elif value is False:
        return 'False'
    return str(value)


def is_email_address(text):
    """
    Return true if it is a valid email address
    """
    return re.search(r'[\w\.-]+@[\w\.-]+', text)


def anonymizer(text):
    """
    A part-of-speech tagger, or POS-tagger, processes a sequence
    of words, and attaches a part of speech tag to each word. See
    https://www.nltk.org/index.html for more information.
    Cant be used ATM. Need to add the process installation to base image.
    $ pip install -U textblob
    $ python -m textblob.download_corpora
    """
    if text is None or text == "":
        new_text = text
    else:
        new_text = []
        # Splits text into sentences
        sentence_list = text.replace('\n', ' ').split(". ")
        for sentence in sentence_list:
            # Splits sentence into list of words and filters empty elts.
            # Not using nltk.word_tokenize as it splits an email address
            # in several entities.
            word_list = list(filter(None, sentence.split(" ")))
            # process word_list
            pos = nltk.pos_tag(word_list)
            new_word_list = []
            for word in pos:
                if is_email_address(word[0]):
                    # tags word as EMAIL
                    new_word_list.append("{EMAIL}")
                elif word[1] == 'NNP':
                    # tags word as NAME (proper noun)
                    new_word_list.append("{NAME}")
                elif word[1] == 'CD':
                    # tags word as NUMBER
                    new_word_list.append("{NUMBER}")
                else:
                    # no tranformation
                    new_word_list.append(word[0])
            new_sentence = " ".join(new_word_list)
            new_text.append(new_sentence)
        new_text = ". ".join(new_text)
    return new_text


def get_date(window, date_format="%Y-%m-%d"):
    """
    Returns date in string format ('%Y-%m-%d') from today using window in days.
    get_date(window=0) returns today, get_date(widnow=1) returns yesterday...
    """
    date = datetime.today() - timedelta(days = window)
    return date.strftime(date_format)

def get_today_date(date_format="%Y-%m-%d"):
    """
    Returns today date in string format specified. Defaults to '%Y-%m-%d'.
    """
    return get_date(0, date_format)

def get_yesterday_date(date_format="%Y-%m-%d"):
    """
    Returns yesterday date in string format specified. Defaults to '%Y-%m-%d'.
    """
    return get_date(1, date_format)


def date_lister(start_date, end_date):
    """
     Returns list of dates between start_date and end_date in string format ('%Y-%m-%d')
     Arguments:
     - start_date (string)
     - end_date (string)
     """
    if end_date < start_date:
        date_list = []
        logging.error("End date must be equal or after start_date")
    else:
        date_list = pd.date_range(start_date, end_date)
        date_list = date_list.format()
        logging.info(date_list)
    return date_list


def validate_date(date, format='%Y-%m-%d', error_msg=None):
    try:
        datetime.strptime(date, format)
    except ValueError:
        if error_msg is None:
            raise ValueError("Incorrect data format, should be {}".format(format))
        else:
            raise ValueError(error_msg)


def level_is_valid(verbosity):
    '''
    Validates key parameter for configure_logging() and raises approriate
    exception messages.

    Returns the expected level for verbosity.
    '''

    if not isinstance(verbosity, str):
        raise ValueError('verbosity needs to be a string')
    verbosity = verbosity.upper()
    logging.info("verbosity passed: %s", verbosity)

    if verbosity not in ('DEBUG', 'INFO', 'QUIET', 'WARNING', 'CRITICAL'):
        raise ValueError('verbosity needs to be one of the following: DEBUG, INFO, QUIET, WARNING, CRITICAL')

    return verbosity


def configure_logging(env=None, verbosity=None):
    """ Sets logging """

    levels = {
        "DEBUG": {
            'level': "DEBUG",
            'format': "%(asctime)s,%(msecs)3d - %(levelname)-8s - %(funcName)5s:%(lineno)-10s :: %(message)s",
            'datefmt': "%Y-%m-%d %H:%M:%S"
        },
        "INFO": {
            'level': "INFO",
            'format': "%(asctime)s - %(levelname)-8s — %(name)-10s :: %(message)s",
            'datefmt': "%H:%M:%S"
        },
        "QUIET": {
            'level': "ERROR",
            'format': "%(asctime)s - %(levelname)-8s — %(name)-10s :: %(message)s",
            'datefmt': "%H:%M:%S"
        },
        "WARNING": {
            'level': "WARNING",
            'format': "%(asctime)s - %(levelname)s - %(funcName)-10s :: %(message)s",
            'datefmt': "%H:%M:%S"
        },
        "CRITICAL": {
            'level': "CRITICAL",
            'format': "%(asctime)s - %(levelname)-8s - %(funcName)5s:%(lineno)-10s :: %(message)s",
            'datefmt': "%Y-%m-%d %H:%M:%S"
        }
    }

    if verbosity is None:
        setlevel = levels["INFO"]['level']
        setformat = levels["INFO"]['format']
        setdatefmt = levels["INFO"]['datefmt']
    else:
        verbosity = level_is_valid(verbosity)
        setlevel = levels[verbosity]['level']
        setformat = levels[verbosity]['format']
        setdatefmt = levels[verbosity]['datefmt']

    logging.config.dictConfig(
        {
            'version': 1,
            'disable_existing_loggers': False,
            'root': {
                'level': setlevel,
                'handlers': ['default']
            },
            'formatters': {
                'default': {
                    'format': setformat,
                    'datefmt': setdatefmt
                },
            },
            'handlers': {
                'default': {
                    'class': 'logging.StreamHandler',
                    'stream': 'ext://sys.stdout',
                    'formatter': 'default',
                    'level': logging.DEBUG
                }
            }
        }
    )


def convert_list_for_sql(my_list):
    """ Convert a python list to a SQL list.
    The function is primarly used when trying to format SQL queries by passing an argument.

    Arguments:
        my_list: list of elements to be used in a SQL query

    Example:
        1. convert_list_for_sql([1, 2, 3]) returns '1, 2, 3'
        2. convert_list_for_sql(['Simone', 'Dan']) returns ''Simone', 'Dan''
    """
    final_list = []
    for item in my_list:
        if isinstance(item, str):
            _item = '\'{}\''.format(item)
        else:
            _item = item
        final_list.append(_item)
    return ", ".join([str(item) for item in final_list])

#       _                         __    __           __   __      __         __
#      (_)___  ____  ____  __  __/ /_  / /___ ______/ /__/ /___ _/ /_  ___  / /
#     / / __ \/ __ \/ __ \/ / / / __ \/ / __ `/ ___/ //_/ / __ `/ __ \/ _ \/ /
#    / / /_/ / / / / / / / /_/ / /_/ / / /_/ / /__/ ,< / / /_/ / /_/ /  __/ /
# __/ /\____/_/ /_/_/ /_/\__, /_.___/_/\__,_/\___/_/|_/_/\__,_/_.___/\___/_/
#/___/                  /____/
"""
author: Johannes Kunze
twitter: @jonnyblacklabel
web: http://www.jonnyblacklabel.de/
github: https://github.com/Jonnyblacklabel
"""


from configparser import ConfigParser
from typing import List
import itertools
import time
import os


def combine_list_values(values: List[str]):
  """get combinations of values in list

  Args:
    values: list of values

  Returns:
    list of combinations
    tuple
  """
  result = []
  for i in range(len(values)):
    result.extend(list(itertools.combinations(values, i+1)))
  return list(filter(None, result))


def dimension_combinations(defaults: List[str],
                           additionals: List[str] = None):
  """get combination of dimensions

  Args:
    defaults: list of fixed values
    additionals: list of values to combine (default: {None})

  Returns:
    list of fixed values combined with permutations
    list of tuples
  """
  dimensions = [defaults]
  combinations = combine_list_values(additionals)
  if len(combinations) > 0:
    dimensions.extend([tuple(sorted(defaults + combination)) for combination in combinations])
  return list(filter(None, dimensions))

def get_tuple(values, sep=','):
  return tuple(sorted(filter(None, values.split(sep)), key=str.lower))

def load(filename: str, section: str = None, path: str = 'configurations'):
  """return config parser

  Args:
    filename: filename without extension
    section: valide section in config file (default: {None})
    path: relative path to config file (default: {'configurations'})

  Returns:
    configparser object
    ConfigParser
  """
  parser = ConfigParser(converters={'tuple':get_tuple})
  parser.read_file(open(os.path.join(path,filename+'.ini')))
  if section:
    return parser[section]
  else:
    return parser

def get_searchtypes():
  parser = load('api_columns')
  return parser['searchtypes'].gettuple('defaults')

def get_dimension_combinations():
  parser = load('api_columns')
  return dimension_combinations(parser['dimensions'].gettuple('defaults'),
                                parser['dimensions'].gettuple('additionals'))

def get_filter_iterators():
  parser = load('api_columns')
  return parser['filters'].gettuple('iterators')

def get_combinations_without_iterators():
  return list(itertools.product(get_searchtypes(),
                                get_dimension_combinations()))

def get_combinations_with_iterators(client, iterator):
  combinations = list(itertools.product(get_searchtypes(),
                                        get_dimension_combinations()))
  result = []
  for combination in combinations:
    values = client.get_iterator_values(iterator, combination[0])
    time.sleep(1)
    filters = [(iterator,value,'equals', ) for value in values]
    for filter_ in filters:
      result.append(combination + (filter_, ))
  return result
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

from sqlite3 import IntegrityError
from loguru import logger
import dataset
import dotenv
import config
import json
import os


dotenv.load_dotenv()

con = dataset.connect('sqlite:///'\
  +os.path.join(os.environ['SQLITE_PATH'],
                os.environ['ROOT_DB']),
  engine_kwargs=dict(connect_args={'check_same_thread':False}))


def init_gsc_properties():
  """Create table gsc_properties"""
  con.query("""
    CREATE TABLE IF NOT EXISTS 'gsc_properties' (
    'id' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    'account_name' TEXT NOT NULL UNIQUE,
    'gsc_property' TEXT NOT NULL UNIQUE,
    'active' BOOLEAN DEFAULT 1
    );
    """)


def drop_gsc_properties():
  """Drop table gsc_properties"""
  con.query("""
    DROP TABLE IF EXISTS 'gsc_properties'
    """)

def init_gsc_property_jobs():
  """Create table gsc_property_jobs"""
  con.query("""
    CREATE TABLE IF NOT EXISTS 'gsc_property_jobs' (
    'id' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    'gsc_property_id' INTEGER NOT NULL,
    'dimensions' TEXT NOT NULL,
    'searchtype' TEXT NOT NULL,
    'filter' TEXT DEFAULT NULL,
    'active' BOOLEAN DEFAULT 1
    );
    """)


def drop_gsc_property_jobs():
  """Drop table gsc_property_jobs"""
  con.query("""
    DROP TABLE IF EXISTS 'gsc_property_jobs'
    """)


def init_query_queue():
  """Create table query_queue"""
  con.query("""
    CREATE TABLE IF NOT EXISTS 'query_queue' (
    'id' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    'gsc_property_id' INTEGER NOT NULL,
    'gsc_property_job_id' INTEGER NOT NULL,
    'date' DATE NOT NULL,
    'attempts' INTEGER NOT NULL DEFAULT 0,
    'finished' BOOLEAN NOT NULL DEFAULT 0,
    'rows' INTEGER NOT NULL DEFAULT 0,
    'streamed' BOOLEAN NOT NULL DEFAULT 0
    );
    """)


def drop_query_queue():
  """Drop table query_queue"""
  con.query("""
    DROP TABLE 'query_queue'
    """)


def up():
  """Init all Tables"""
  init_gsc_properties()
  init_gsc_property_jobs()
  init_query_queue()


def down():
  """Drop all Tables"""
  drop_gsc_properties()
  drop_gsc_property_jobs()
  drop_query_queue()


def delete():
  """Delete Database"""
  os.remove(os.path.join(os.environ['SQLITE_PATH'],os.environ['ROOT_DB']))

def get_gsc_property(account_name: str, gsc_property: str):
  """get item from gsc properties table

  Args:
    account_name: account name (str)
    gsc_property: gsc property (str)

  Returns:
    database row
    OrderedDict
  """
  return con['gsc_properties'].find_one(account_name=account_name,
                                        gsc_property=gsc_property)


def get_gsc_properties(**kwargs):
  """get items from gsc properties table

  Args:
    kwargs: where condition on columns

  Returns:
    database rows
    dataset.util.ResultIter
  """
  return con['gsc_properties'].find(**kwargs)

def create_gsc_property(account_name: str, gsc_property: str):
  """creates item in gsc properties table

  Args:
    account_name: account name (str)
    gsc_property: gsc property (str)

  Returns:
    ids of created rows
    list
  """
  init_gsc_properties()
  try:
    p_key = con['gsc_properties'].insert(dict(account_name = account_name,
                                              gsc_property = gsc_property))
  except IntegrityError:
    logger.info(f'{account_name} and {gsc_property} exists.')
    p_key = con['gsc_properties'] \
              .find_one(account_name=account_name,
                        gsc_property=gsc_property)['id']
  else:
    logger.info(f'{account_name} and {gsc_property} added.')
  finally:
    return p_key


def update_gsc_property(account_name: str, gsc_property: str, active: bool = True):
  """updates item in gsc properties table

  Args:
    account_name: account name (str)
    gsc_property: gsc property (str)
    active: job active or inactive (default: {True})

  Returns:
    inserted row or row count if update
    OrderedDict or int
  """
  data = dict(account_name=account_name,
              gsc_property=gsc_property,
              active=active)
  keys = ['account_name','gsc_property']
  return con['gsc_properties'].upsert(row=data, keys=keys)


def delete_gsc_property(p_key: int):
  """delete item in gsc properties table

  Args:
    p_key: primary key

  Returns:
    row count or false if not exists
    int or bool
  """
  con['gsc_properties'].delete(id=p_key)
  return p_key


def get_gsc_property_job(p_key: int, **kwargs):
  """get item from gsc property jobs table

  Args:
    p_key: primary id
    kwargs: where on columns

  Returns:
    database row
    OrderedDict
  """
  return con['gsc_property_jobs'].find_one(id=p_key, **kwargs)


def get_gsc_property_jobs(gsc_property_id: int, **kwargs):
  """get items from gsc property jobs table for foreign key

  Args:
    gsc_property_id: foreign key
    kwargs: where on columns

  Returns:
    database rows
    dataset.util.ResultIter
  """
  return con['gsc_property_jobs'].find(gsc_property_id=gsc_property_id, **kwargs)

def create_gsc_property_job(gsc_property_id: int, dimensions: str,
                            searchtype: str, **kwargs):
  """creates item in gsc property jobs table

  Args:
    gsc_property_id: foreign key
    kwargs: where on columns

  Returns:
    id of inserted row
    int
  """
  init_gsc_property_jobs()

  return con['gsc_property_jobs'].insert(dict(gsc_property_id = gsc_property_id,
                                              dimensions = dimensions,
                                              searchtype = searchtype,
                                              **kwargs))


def update_gsc_property_job(p_key: int, active: bool):
  """insert or update item in gsc property jobs table

  Args:
    p_key: primary key
    active: job active or inactive

  Returns:
    inserted row or row count if update
    OrderedDict or int
  """
  data = dict(id=p_key,
              active=active)
  keys = ['id']
  return con['gsc_rpoerty_jobs'].upsert(row=data, keys=keys)

def update_gsc_property_jobs(gsc_property_id: int, active: bool):
  """inserts or updates items in gsc property jobs table

  Args:
    gsc_property_id: foreign key
    active: job active or inactive

  Returns:
    inserted row or row count if update
    OrderedDict or int
  """
  data = dict(gsc_property_id=gsc_property_id,
              active=active)
  keys = ['gsc_property_id']
  return con['gsc_rpoerty_jobs'].upsert(row=data, keys=keys)


def delete_gsc_property_job(p_key: int):
  """delete item in gsc property jobs table

  Args:
    p_key: primary key

  Returns:
    row count or false if not exists
    int or bool
  """
  return con['gsc_property_jobs'].delete(id=p_key)


def delete_gsc_property_jobs(gsc_property_id: int):
  """delete items in gsc property jobs tables

  Args:
    gsc_property_id: foreign key

  Returns:
    row count or false if not exists
    int or bool
  """
  return con['gsc_property_jobs'].delete(gsc_property_id = gsc_property_id)


def get_query_queue_item(p_key: int, **kwargs):
  """get item from query queue table

  Args:
    p_key: primary key
    kwargs: where on columns

  Returns:
    database row
    OrderedDict
  """
  return con['query_queue'].find_one(id=p_key, **kwargs)


def get_query_queue_items(gsc_property_id: int, gsc_property_job_id: int,
                          **kwargs):
  """get items from query queue table

  Args:
    gsc_property_id: id of gsc property
    gsc_property_job_id: id of property job
    kwargs: where on columns

  Returns:
    database rows
    dataset.util.ResultIter
  """
  return con['query_queue'].find(gsc_property_id = gsc_property_id,
                                 gsc_property_job_id = gsc_property_job_id,
                                 **kwargs)


def delete_query_queue_item(p_key: int):
  """delete item in query queue table

  Args:
    p_key: primary key

  Returns:
    row count or false if not exists
    int or bool
  """
  return con['query_queue'].delete(id=p_key)


def delete_query_queue_items(gsc_property_id: int):
  """delete items in query queue tables

  Args:
    gsc_property_id: foreign key

  Returns:
    row count or false if not exists
    int or bool
  """
  return con['query_queue'].delete(gsc_property_id = gsc_property_id)


def update_query_queue_item(p_key: int, **kwargs):
  """insert or update query queue item

  Args:
    p_key: primary key
    attempts: number of attemts
    finished: true or false
    streamed: streamed to big query

  Returns:
    inserted row or row count if update
    OrderedDict or int
  """
  data = dict(id=p_key, **kwargs)
  keys = ['id']
  return con['query_queue'].upsert(row=data, keys=keys)


def update_query_queue_items(gsc_property_id, gsc_property_job: int,
                             attempts: int, finished: bool, streamed: bool):
  """insert or update query queue items

  Args:
    gsc_property_id: id of gsc property
    gsc_property_job_id: id of property job
    attempts: number of attemts
    finished: true or false
    streamed: streamed to big query

  Returns:
    inserted row or row count if update
    OrderedDict or int
  """
  data = dict(gsc_property_id=gsc_property_id,
              gsc_property_job_id=gsc_property_job_id,
              attempts=attempts,
              finished=finished,
              streamed=streamed)
  keys = ['gsc_property_id','gsc_property_job_id']
  return con['query_queue'].upsert(row=data, keys=keys)


def main():
  from argparse import ArgumentParser
  parser = ArgumentParser(description='handling database')
  subparsers = parser.add_subparsers(title='commands')
  up = subparsers.add_parser('up',
                             help='init all tables')
  up.set_defaults(func=up)
  down = subparsers.add_parser('down',
                               help='delete all tables')
  down.set_defaults(func=down)
  delete = subparsers.add_parser('delete',
                                 help='delete db')
  delete.set_defaults(func=delete)

  args = parser.parse_args()
  args.func(**{k: v for k,v in vars(args).items() if k is not 'func'})

if __name__ == '__main__':
  main()

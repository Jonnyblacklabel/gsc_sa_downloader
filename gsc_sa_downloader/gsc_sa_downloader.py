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

from searchanalytics import Client, QueryThreaded
from datetime import datetime
from loguru import logger
from typing import List
from tqdm import tqdm
import config
import json
import time
import sys
import db
import os

def generate_queries(client: Client, account_name: str,
                     gsc_property: str, p_key: int, j_keys: List[int]):
  db.init_query_queue()
  length = 0
  for j_key in tqdm(j_keys, desc='jobs'):
    data = []
    job = db.get_gsc_property_job(j_key)
    dates = client.get_date_list(gsc_property, searchtype=job['searchtype'])
    dates = [datetime.strptime(date, '%Y-%m-%d').date() for date in dates]
    dates_from_db = [row['date'] for row in
                     db.get_query_queue_items(p_key, j_key)]
    if len(dates_from_db) > 0:
      dates = [x for x in dates if x not in dates_from_db]
    for date in dates:
      data.append(dict(gsc_property_id = p_key,
                       gsc_property_job_id = j_key,
                       date = date))
    db.con['query_queue'].insert_many(data)
    length += len(data)
  logger.info(f'inserted {length} items in query_queue')


def create_account_and_property(account_name, gsc_property, reset=False):
  """add or reset account with property

  (new) account with (new) property is inserted into database.
  if reset → data database will be deleted, jobs will be reset

  Args:
    account_name: name of account (credentials filename)
    gsc_property: gsc property (with trailing slash)
    reset: delete data sqlite and delete row in root db (default: {False})
  """
  client = Client(account_name = account_name)
  client.set_webproperty(gsc_property)
  # löschen der daten sqlite des accounts
  if reset:
    try:
      sqlite_data = os.path.join(os.environ['SQLITE_PATH'], account_name+'.db')
      logger.info(f'deleting database {sqlite_data}.')
      os.remove(sqlite_data)
    except FileNotFoundError:
      logger.info('no sqlite db found.')
    try:
      gsc_property_id = db.get_gsc_property(account_name=account_name,
                                            gsc_property=gsc_property)['id']
      logger.info(f'removing queue items for {gsc_property}')
      db.delete_query_queue_items(gsc_property_id)
      logger.info(f'removing jobs for {gsc_property}')
      db.delete_gsc_property_jobs(gsc_property_id)
      logger.info(f'removing {account_name} with {gsc_property} from database')
      db.delete_gsc_property(account_name, gsc_property)
    except TypeError:
      logger.info('cannot delete')

  # erstellen neuer einträge
  # gsc property
  try:
    gsc_property_id = db.get_gsc_property(account_name, gsc_property)['id']
  except TypeError:
    logger.info(f'create {account_name} with {gsc_property} in database.')
    gsc_property_id = db.create_gsc_property(account_name, gsc_property)
  # property jobs
  job_keys = [row['id'] for row in db.get_gsc_property_jobs(gsc_property_id)]
  if len(job_keys) == 0:
    logger.info(f'create job definitions for {gsc_property} without iterators')
    combinations = config.get_combinations_without_iterators()
    job_keys = []
    for combination in tqdm(combinations, desc='jobs'):
      id_ = db.create_gsc_property_job(gsc_property_id = gsc_property_id,
                                       searchtype = combination[0],
                                       dimensions = json.dumps(combination[1]))
      job_keys.append(id_)

    logger.info(f'create job definitions for {gsc_property} with iterators')
    iterators = config.get_filter_iterators()
    combinations_iterators = []
    for iterator in tqdm(iterators, desc='creating iterations'):
      combinations_iterators.extend(config.get_combinations_with_iterators(client, iterator))
    # job_keys = []
    for combination in tqdm(combinations_iterators, desc='inserting iterators'):
      id_ = db.create_gsc_property_job(gsc_property_id = gsc_property_id,
                                       searchtype = combination[0],
                                       dimensions = json.dumps(combination[1]),
                                       filter = json.dumps(combination[2]))
      job_keys.append(id_)
  logger.info('genereating daily queries in database.')
  # query jobs
  generate_queries(client, account_name, gsc_property,
                   gsc_property_id, job_keys)

def download(account_name, gsc_property, generate=False, reset=False, max_workers=5):
  """download gsc searchanalytics data

  download gsc searchanalytics data for gsc property.
  will create new jobs for property.

  Args:
    account_name: name of account (credentials filename)
    gsc_property: gsc property (with trailing slash)
    generate: if True, generate new queue items
    reset: delete data sqlite and delete row in root db (default: {False})
  """
  if generate or reset:
    create_account_and_property(account_name=account_name,
                               gsc_property=gsc_property,
                               reset=reset)

  logger.info(f'starting download for {account_name} with {gsc_property}.')

  properties = db.get_gsc_properties(account_name = account_name,
                                     gsc_property = gsc_property,
                                     active = True)

  for property_ in tqdm(list(properties), desc='properties'):
    jobs = db.get_gsc_property_jobs(property_['id'], active=True)


    for job in tqdm(list(jobs), desc='jobs', leave=False):
      queue = db.get_query_queue_items(property_['id'],
                                       job['id'],
                                       finished = False,
                                       attempts = {'<=': 5})
      thread_queue_items = []
      for item in queue:
        try:
          table_name = job['searchtype'] + '_' + '_'.join(json.loads(job['dimensions']))
          if job['filter'] is not None:
            filter_ = json.loads(job['filter'])
            table_name = '_'.join([table_name, filter_[1].lower()])
          thread_queue_items.append(dict(tbl_name=table_name,
                                         query=item,
                                         job=job))
        except Exception as e:
          logger.error(f'error: {e}')

      logger.info(f'starting threaded fetching - [max_workers {max_workers}]')
      query_threaded = QueryThreaded(account_name = property_['account_name'],
                                     gsc_property = property_['gsc_property'],
                                     items = thread_queue_items,
                                     max_workers = max_workers)
      query_threaded.run()
      logger.info('finished threaded fetching')

  logger.info(f'finsihed download for {account_name} with {gsc_property}.')

def download_all(generate=False, reset=False, max_workers=5):
  """download gsc searchanalytics data for all properties

  !!! ALL Databases are deleted and cleard if reset=True

  Args:
    reset: delete data sqlite and delete row in root db (default: {False})
  """

  logger.info('starting download for all properties')

  for row in db.con['gsc_properties'].all():
    download(row['account_name'], row['gsc_property'], reset=reset)

  logger.info('finished download for all properties')


def main():
  logger.add('logs/{time:YYYY-MM-DD}.log', level='DEBUG', backtrace=True,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
  from argparse import ArgumentParser
  parser = ArgumentParser(description='run searchanalytics data downloader')
  parser.add_argument('--reset', '-r', action='store_true',
                      help='clear controle tables & delete account database')

  subparsers = parser.add_subparsers(title='commands')

  dl_all = subparsers.add_parser('download_all',
                                 help='generate queries & download')
  dl_all.set_defaults(func=download_all)

  dl = subparsers.add_parser('download',
                             help='create/generate queries & download for property of account')
  dl.set_defaults(func=download)

  for sp in [dl_all, dl]:
    sp.add_argument('--generate', '-g', action='store_true',
                    help='generate queue items for new dates')
    sp.add_argument('--max_workers', '-w', type=int, default=10,
                    help='number of max_workers')

  ga = subparsers.add_parser('create-account',
                             help='create/generate queries for property of account')
  ga.set_defaults(func=create_account_and_property)

  for sp in [dl,ga]:
    sp.add_argument('account_name', help='name of account')
    sp.add_argument('gsc_property', help='name of gsc property')

  args = parser.parse_args()
  args.func(**{k: v for k,v in vars(args).items() if k is not 'func'})

if __name__ == '__main__':
  main()

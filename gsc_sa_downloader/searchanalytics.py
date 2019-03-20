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

from searchconsole.query import Query, Report
from searchconsole.account import Account
from threading import Thread, get_ident
from apiclient import discovery
import googleapiclient.errors
from retrying import retry
from loguru import logger
from queue import Queue
from math import ceil
import numpy as np
import dataset
import random
import time
import json
import time
import auth
import db
import os


def patch_wait(seconds):
  def _wait(self):
    now = time.time()
    elapsed = now - self._lock
    wait = max(0, seconds - elapsed)
    time.sleep(wait)
    self._lock = time.time()

    return wait
  Query._wait = _wait

def patch_execute():
  def execute(self):
    raw = self.build()
    url = self.api.url
    try:
      response = self.api.account.service.searchanalytics().query(
        siteUrl=url, body=raw).execute()
      self._wait() # put self._wait at the end so first call waits
    except googleapiclient.errors.HttpError as e:
      raise e
    return Report(response, self)
  Query.execute = execute


class Client:

  def __init__(self, account_name, verbose=False):
    credentials = self.get_credentials(account_name)
    service = discovery.build('webmasters','v3',credentials=credentials)
    self.account = Account(service, credentials)
    self.verbose = verbose
    self.months = int(os.environ['MONTHS'])


  @staticmethod
  def get_credentials(account_name):
    return auth.authenticate_gsc(account=account_name,
                                 client_id=os.environ['CLIENT_ID'],
                                 client_secret=os.environ['CLIENT_SECRET'])


  def get_account(self):
    return self.account

  def set_webproperty(self, gsc_property):
    self.webproperty = self.account[gsc_property]

  def get_webproperty(self):
    return self.webproperty


  @retry(stop_max_attempt_number=5,
         wait_exponential_multiplier=1000,
         wait_exponential_max=10000)
  def get_date_list(self, webproperty, searchtype='web'):
    report = self.webproperty \
                 .query.range('today', months=-(self.months)-1) \
                 .dimension('date') \
                 .search_type(searchtype) \
                 .get()
    return [row.date for row in report.rows]


  @retry(stop_max_attempt_number=5,
         wait_exponential_multiplier=1000,
         wait_exponential_max=10000)
  def get_iterator_values(self, iterator_dimension, searchtype):
    report = self.webproperty \
                 .query.range('today', months=-(self.months)-1) \
                 .dimension(iterator_dimension) \
                 .search_type(searchtype) \
                 .get()
    return [row[0] for row in report.rows]


  def query_queue_item(self, item, job):
    query = self.webproperty.query.range(start=item['date'],
                                         stop=item['date']) \
                              .dimension(*json.loads(job['dimensions'])) \
                              .search_type(job['searchtype'])
    if job['filter'] is not None:
      filter_ = json.loads(job['filter'])
      query.filter(dimension = filter_[0],
                    expression = filter_[1],
                    operator = filter_[2])
    return query


class QueryThreaded:

  def __init__(self, account_name, gsc_property, items, max_workers=10, rps=3):
    patch_wait(1/rps) # wait n seconds (api rps)
    patch_execute() # wait on first iteration
    self.account_name = account_name
    self.gsc_property = gsc_property
    self.tasks = items
    self.max_workers = max_workers
    self.target_rps = rps
    self.look_back = 1
    self.elapsed = []
    self.hits = []
    self.db_queue = Queue()
    self.task_queue = Queue()
    self.worker_threads = []
    self.to_break = []


  @staticmethod
  @retry(stop_max_attempt_number=5,
         wait_exponential_multiplier=1000,
         wait_exponential_max=10000)
  def run_query(query):
    return query.get()


  def fill_task_queue(self):
    for task in self.tasks:
      self.task_queue.put(task)


  def add_worker(self, n=1):
    for i in range(n):
      thread = Thread(target=self.task_execute)
      thread.setDaemon(True)
      thread.start()
      self.worker_threads.append(thread)


  def remove_worker(self):
    thread = self.worker_threads[0]
    self.to_break.append(thread.ident)


  def start_task_workers(self):
    for i in range(self.max_workers):
      t = Thread(target=self.task_execute)
      t.setDaemon(True)
      t.start()
      self.worker_threads.append(t)


  def stop_task_workers(self):
    for thread in self.worker_threads:
      self.task_queue.put(None)
    for thread in self.worker_threads:
      thread.join()

  def mean_rps(self):
    # calc mean rps
    try:
      return (np.sum(self.hits[-self.look_back:]) / np.sum(self.elapsed[-self.look_back:]))*len(self.worker_threads) # mean rps last n calls
    except Exception:
      return 1


  def throttle_worker(self):
    self.add_worker()
    task_queue_size_start = self.task_queue.qsize()
    tasks_done_last_60_seconds = 0
    tasks_done_before_60_seconds = 0
    start = time.time()
    while not self.task_queue.empty():
      # remove dead threads from list
      for thread in self.worker_threads:
        if not thread.is_alive():
            self.worker_threads.remove(thread)

      # queue size
      task_queue_size = self.task_queue.qsize()
      tasks_done = task_queue_size_start - task_queue_size

      if time.time() - start >= 60: # after 60 seconds
        tasks_done_last_60_seconds = tasks_done - tasks_done_before_60_seconds
        tasks_done_before_60_seconds += tasks_done_last_60_seconds

        self.look_back = tasks_done_last_60_seconds # window for mean rps calc (default 10)
        # dampen last 60 hits because every worker does pagination.
        # one does not know for shure if all hits were in 60 seconds window.
        # hits_last_60_seconds = ceil(np.sum(self.hits[-self.look_back:]) / 2**(np.log10(len(self.worker_threads))/10))
        hits_last_60_seconds = np.sum(self.hits[-self.look_back:])

        # adding workers
        if 0 < (hits_last_60_seconds + (hits_last_60_seconds / len(self.worker_threads))) <= 60 * self.target_rps:
          if len(self.worker_threads) < self.max_workers \
          and len(self.worker_threads) < self.task_queue.qsize(): # max not reached
            logger.info(f'hits in 60 seconds [{hits_last_60_seconds}] - adding 1 worker')
            self.add_worker()
            # time.sleep(2**len(self.worker_threads)*1) # wait based on len workers

        # removing workers
        if hits_last_60_seconds > 60 * self.target_rps \
        and len(self.worker_threads) > 1: # rps to high, remove worker
          logger.info(f'hits in 60 seconds [{hits_last_60_seconds}] - removing 1 worker')
          self.remove_worker()

        time.sleep(.5)
        start = time.time() # reset start time

    # ending workers
    self.task_queue.join() # wait till queue is done
    self.stop_task_workers()


  def task_execute(self):
    client = Client(self.account_name)
    client.set_webproperty(self.gsc_property)
    n_errors = 0
    while True:
      if get_ident() in self.to_break: # break worker if in to break
        break
      item = self.task_queue.get()
      if item is None: # break worker if None item in queue
        break
      try:
        start = time.time()
        query = client.query_queue_item(item['query'], item['job']) # build query
        query._lock = start # set lock for searchconsole client
        report = self.run_query(query) # run query
        dict_rows = report.to_dict()
        len_rows = len(report)
        hits = max(2, ceil((len_rows / 25000)+1))
        elapsed = time.time() - start
        rps = hits / elapsed
        self.elapsed.append(elapsed) # add to object elapsed
        self.hits.append(hits) # add to object hits
        for row in dict_rows:
          row.update(dict(date=item['query']['date'],
                          query_queue_id=item['query']['id']))
      except googleapiclient.errors.HttpError as e:
        n_errors += 1
        logger.error(e)
        if n_errors < 2:
          logger.warning(f'error [{n_errors}] - waiting for 20 Minutes to continue.')
          time.sleep(60*20)
        else:
          logger.warning(f'error [{n_errors}] - exit - try again later / tomorrow.')
          break
      except Exception as e:
        logger.exception(e)
        break
      else:
        mean_rps = self.mean_rps()
        logger.info(f'[{len(self.worker_threads)}] worker - [{round(rps,3)}] rps - [{round(mean_rps,3)}] mean rps - [{len(report)}] rows - [{hits}] hits - {item["query"]["date"]} - {item["job"]["dimensions"]} - {item["job"]["searchtype"]} - {item["job"]["filter"]}')
        self.db_queue.put(dict(tbl_name=item['tbl_name'],
                               report=dict_rows,
                               query_queue_id=item['query']['id'],
                               attempts=item['query']['attempts'],
                               report_len=len_rows,
                               elapsed=elapsed,
                               hits=hits,
                               rps=rps))
      finally:
        self.task_queue.task_done()



  def db_writer(self):
    t_db = dataset.connect('sqlite:///' \
                           +os.path.join(os.environ['SQLITE_PATH'],\
                                         self.account_name+'.db'),
                           engine_kwargs=dict(connect_args={'check_same_thread':False}))
    time.sleep(10)
    while self.worker_threads or not self.db_queue.empty():
      if not self.db_queue.empty():
        item = self.db_queue.get()
        if item is None:
          del t_db
          break
        table = t_db[item['tbl_name']]

        table.insert_many(item['report'])
        db.update_query_queue_item(item['query_queue_id'],
                                   attempts=item['attempts']+1,
                                   finished=True,
                                   rows=item['report_len'],
                                   seconds=item['elapsed'],
                                   hits=item['hits'],
                                   rps=item['rps'])
        self.db_queue.task_done()
      elif self.worker_threads:
        time.sleep(5)
      else:
        del t_db

  def drop_indices(self):
    table = self.tasks[0]['tbl_name']
    logger.info(f'dropping indices for {table}')
    t_db = dataset.connect('sqlite:///' \
                           +os.path.join(os.environ['SQLITE_PATH'],\
                                         self.account_name+'.db'),
                           engine_kwargs=dict(connect_args={'check_same_thread':False}))
    drop_index_qqid = f'DROP INDEX IF EXISTS {table}_query_queue_id_idx;'
    drop_index_date = f'DROP INDEX IF EXISTS {table}_date_idx;'
    t_db.query(drop_index_qqid)
    t_db.query(drop_index_date)
    del t_db


  def create_indices(self):
    table = self.tasks[0]['tbl_name']
    logger.info(f'creating indices for {table}')
    t_db = dataset.connect('sqlite:///' \
                           +os.path.join(os.environ['SQLITE_PATH'],\
                                         self.account_name+'.db'),
                           engine_kwargs=dict(connect_args={'check_same_thread':False}))
    create_index_qqid = f'CREATE INDEX IF NOT EXISTS {table}_query_queue_id_idx ON {table} (query_queue_id);'
    create_index_date = f'CREATE INDEX IF NOT EXISTS {table}_date_idx ON {table} (date);'
    t_db.query(create_index_qqid)
    t_db.query(create_index_date)
    del t_db




  def run(self):
    if len(self.tasks) > 0:
      needs_new_indices = False
      if len(self.tasks) > 50:
        self.drop_indices()
        needs_new_indices = True

      self.fill_task_queue()

      db_thread = Thread(target=self.db_writer)
      db_thread.setDaemon(True)
      db_thread.start()

      self.throttle_worker()

      self.db_queue.join()
      self.db_queue.put(None)
      db_thread.join()

      if needs_new_indices:
        self.create_indices()
    else:
      logger.info('nothing to fetch.')
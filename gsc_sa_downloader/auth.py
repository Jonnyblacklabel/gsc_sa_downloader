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


import pydata_google_auth


def authenticate_gsc(account: str, client_id: str, client_secret: str,
                     auth_local_webserver: bool = True):
  cache = pydata_google_auth.cache.ReadWriteCredentialsCache(dirname='gsc-sa-downloader/gsc',
                                                             filename=account)
  scopes = ['https://www.googleapis.com/auth/webmasters']
  credentials = pydata_google_auth.get_user_credentials(scopes,
                                                        client_id=client_id,
                                                        client_secret=client_secret,
                                                        credentials_cache=cache,
                                                        auth_local_webserver=auth_local_webserver)
  return credentials


def authenticate_gcloud(account: str, auth_local_webserver: bool = True):
  cache = pydata_google_auth.cache.ReadWriteCredentialsCache(dirname='gsc-sa-downloader/gcloud',
                                                             filename=account)
  scopes = ['https://www.googleapis.com/auth/cloud-platform']
  credentials = pydata_google_auth.get_user_credentials(scopes,
                                                        credentials_cache=cache,
                                                        auth_local_webserver=auth_local_webserver)
  return credentials


def main():
  import dotenv, os
  from argparse import ArgumentParser
  dotenv.load_dotenv()

  parser = ArgumentParser(description='authenticate google stuff')
  parser.add_argument('--account', '-a', default='gsc_sa_downloader',
                      help='account identifier (default "gsc_sa_downloader")')
  parser.add_argument('--console', '-c', action='store_false',
                      dest='auth_local_webserver',
                      help='auth flow via console (default is browser)')

  subparsers = parser.add_subparsers(title='commands')

  gcloud = subparsers.add_parser('gcloud',
                                 help='authenticate google cloud')
  gcloud.set_defaults(func=authenticate_gcloud)

  gsc = subparsers.add_parser('gsc',
                              help='authenticate google search console')
  gsc.set_defaults(func=authenticate_gsc)
  gsc.add_argument('--client_id', default=os.environ['CLIENT_ID'],
                   help='client id for google api project (default in .env)')
  gsc.add_argument('--client_secret', default=os.environ['CLIENT_SECRET'],
                   help='client secret for google api project (default in .env)')

  args = parser.parse_args()
  args.func(**{k: v for k,v in vars(args).items() if k is not 'func'})

if __name__ == '__main__':
  main()
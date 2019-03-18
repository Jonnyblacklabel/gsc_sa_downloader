# Google Search Console - Searchanalytics Downloader

Download Searchanalytics data from Google Search Console into local sqlite database.
It uses the `google-searchconsole` Package [google-searchconsole](https://github.com/joshcarty/google-searchconsole) from joshcarty.

gsc-sa-downloader checks the api for valide dates and makes api calls for those dates with all combinations.
It also keeps track of all finished api calls. If something breaks, one can continue by restarting.

One sqlite database for accounts and query definition is created. While the download process is running one sqlite database per account is created.

*The download process can take very long if your account is large*

## Installation
- Download or clone the repository
- Python 3.7 and pipenv is needed
- run `pipenv install`

## CLI
One can start the script via simple cli-commands.
```
# first generate api calls for all days, then start downloading.
python gsc_sa_downloader.py download [account_name] [gsc_property] --generate
```

## Combination of Searchanalytics Dimension
All combinations of dimensions are build based on the `api_columns.ini` file inside the configurations-folder.
```
[dimensions]
defaults=country,device → defaults are always present
additionals=page,query  → additionals are combined with defaults

[searchtypes]
defaults=image,video,web → all searchtypes are downloaded for all dimension-combinations

[filters]
iterators=searchAppearance → all searchtype / dimension-combinations are downloaded with all searchAppearance Filters
```

## Environment Variables
```
LOGURU_FORMAT="<green>{time:YYYY-MM-DD HH:mm:ss}</green>: <level>{message}</level>"
SQLITE_PATH=C:\Users\UserName\Temp
ROOT_DB=gsc_sa_downloader.db
CLIENT_ID=[Client ID des Google API Projekts]
CLIENT_SECRET=[Clientschlüssel des Google API Projekts]
MONTHS=16
```

## To Do
- Stream data to Google Bigquery
- Other database than sqlite
- sqlite database per property, not per account
- Normalization
- Better throttling for threaded api calls

https://developers.google.com/resources/api-libraries/documentation/webmasters/v3/python/latest/
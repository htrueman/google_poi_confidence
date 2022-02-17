# Google POI Confidence - Coding Challenge

Solution for Kuwala coding challenge.

Used Python and BigQuery as main tools.

BQ dataset may be checked [there](https://console.cloud.google.com/bigquery?project=poi-confidence&d=poi_dataset&p=poi-confidence&page=dataset) 
(it's public read and would be pinned in your BQ projects list).

## Startup guide
- Get GCP service creds json file;
- Set `GOOGLE_APPLICATION_CREDENTIALS` env variable with path to file above;
- Put csv data files in `csv_data/`
(or anywhere you want, but then script params are required to set. See `main.py`)
- Install requirements from `requirements.txt`;
- Start script with `python main.py --load-data=True` if starting the first time. 
Start with `python main.py` if starting the 2nd and next times;
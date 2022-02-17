import argparse
import csv
import logging
import os

import google.api_core.exceptions
from google.cloud import bigquery

CSV_PATH = "csv_data"
client = bigquery.Client()
logger = logging.getLogger(__name__)


def get_table_id(csv_file):
    table_name, _ = os.path.splitext(os.path.basename(csv_file))
    table_id = f"poi-confidence.poi_dataset.{table_name}"
    return table_id


def load_to_bigquery(csv_file):
    table_id = get_table_id(csv_file)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=";",
        allow_quoted_newlines=True,
        write_disposition=bigquery.job.WriteDisposition.WRITE_EMPTY,
    )
    try:
        load_job = client.load_table_from_file(
            file_obj=open(csv_file, "rb"), destination=table_id, job_config=job_config
        )
        load_job.result()
        destination_table = client.get_table(table_id)
        logger.info(f"Loaded {destination_table.num_rows} rows into {table_id}.")
    except google.api_core.exceptions.Conflict as e:
        logger.warning(e)


def add_confidence_column(csv_file):
    table_id = get_table_id(csv_file)
    table = client.get_table(table_id)

    schema = table.schema
    confidence_field = bigquery.SchemaField("confidence_score", "FLOAT")
    if confidence_field not in schema:
        schema.append(confidence_field)
        table.schema = schema
        try:
            client.update_table(table, ["schema"])
        except google.api_core.exceptions.BadRequest as e:
            logger.warning(e)


def update_poi_confidence():
    """
    Calculate POI confidence with next formulae:
        confidence_score_1 = 1 - (location_diff - LOCATION_DIFF_MIN) / (LOCATION_DIFF_MAX - LOCATION_DIFF_MIN)
        confidence_score_2 = levenshtein_set_ratio(gpoi.name, opoi.name)
        confidence_score_3 = levenshtein_set_ratio(gpoi.name, opoi.tags)
        confidence_score_4 = levenshtein_set_ratio(gpoi.address, opoi.tags)

        confidence_score = (confidence_score_1 + confidence_score_2 + confidence_score_3 + confidence_score_4) / 4

    confidence_score_1 is actually the most important param, but the rest are correlate well with it.
    So calculating confidence_score param with equal weight for each feature.
    """

    update_query = """
        DECLARE location_diffs 
        DEFAULT (SELECT AS STRUCT * FROM `poi-confidence.poi_dataset.min_max_location_view` LIMIT 1);
        
        UPDATE `poi-confidence.poi_dataset.google_osm_poi_matching` poi_matching 
        SET confidence_score = (location_diff + name_diff + address_tags_diff + categories_diff) / 4
        FROM
        (
            SELECT
                poi_matching.internal_id,
                (1 - (ST_DISTANCE(ST_GEOGPOINT(gpoi.longitude, gpoi.latitude), 
                    ST_GEOGPOINT(opoi.longitude, opoi.latitude)) - location_diffs.min_location_diff) 
                    / (location_diffs.max_location_diff - location_diffs.min_location_diff)
                ) location_diff,
                `poi-confidence.poi_dataset.levenshtein_set_ratio`(gpoi.name, opoi.name) / 100 name_diff,
                `poi-confidence.poi_dataset.levenshtein_set_ratio`(gpoi.name, opoi.tags) / 100 name_tags_diff,
                `poi-confidence.poi_dataset.levenshtein_set_ratio`(gpoi.address, opoi.tags) / 100 address_tags_diff,
                `poi-confidence.poi_dataset.levenshtein_set_ratio`(
                    gpoi.categories, opoi.categories) / 100 categories_diff,
            FROM `poi-confidence.poi_dataset.google_poi` gpoi 
            JOIN `poi-confidence.poi_dataset.google_osm_poi_matching` poi_matching 
            ON gpoi.internal_id = poi_matching.internal_id
            JOIN `poi-confidence.poi_dataset.osm_poi` opoi
            ON poi_matching.osm_id = opoi.osm_id
        ) poi_confidence_table
        WHERE poi_confidence_table.internal_id = poi_matching.internal_id;
    """
    client.query(update_query)


def export_to_csv():
    read_query = """
        SELECT * FROM `poi-confidence.poi_dataset.google_osm_poi_matching` ORDER BY confidence_score DESC
    """
    read_query_job = client.query(read_query)
    rows = read_query_job.result()
    filename = f"{CSV_PATH}/google_osm_poi_matching_with_confidence.csv"
    with open(filename, "a+") as f:
        writer = csv.DictWriter(f, fieldnames=[field.name for field in rows.schema])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    logger.info(f"Exported to {filename}")


def main(load_data, osm_poi, google_poi, google_osm_poi_matching):
    if load_data:
        load_to_bigquery(osm_poi)
        load_to_bigquery(google_poi)
        load_to_bigquery(google_osm_poi_matching)

    add_confidence_column(google_osm_poi_matching)
    update_poi_confidence()
    export_to_csv()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some integers.")
    # Set to True if starting the first time
    parser.add_argument("--load_data", default=False, type=bool)
    parser.add_argument(
        "--osm_poi", default=f"{CSV_PATH}/osm_poi.csv", help="osm_poi.csv file path"
    )
    parser.add_argument(
        "--google_poi",
        default=f"{CSV_PATH}/google_poi.csv",
        help="google_poi.csv file path",
    )
    parser.add_argument(
        "--google_osm_poi_matching",
        default=f"{CSV_PATH}/google_osm_poi_matching.csv",
        help="google_osm_poi_matching.csv file path",
    )
    args = parser.parse_args()

    main(
        load_data=args.load_data,
        osm_poi=args.osm_poi,
        google_poi=args.google_poi,
        google_osm_poi_matching=args.google_osm_poi_matching,
    )

import os
import pandas as pd
import polars as pl
from elasticsearch import Elasticsearch, helpers
import json
import glob
import re
from charset_normalizer import detect
from tqdm import tqdm
import argparse

# Set up Elasticsearch client
es_client = Elasticsearch("http://localhost:9200")

def index_csv_files():
    """Index CSV files to Elasticsearch.

    Parameters:
        --csv (str): Location of the CSV file(s)
        --json-field (str): Field with JSON data
        --timestamp-field (str): Column to use as the timestamp field
        --index-name (str): Name for the index
        --time-format-string (str): Format string for the timestamp field

    Returns:
        None
    """

    # Argument parser setup
    parser = argparse.ArgumentParser(description='Index CSV files to Elasticsearch')
    parser.add_argument('--csv', type=str, default='', help='Location of the CSV file(s)')
    parser.add_argument('--json-field', type=str, default='', help='Field with JSON data')
    parser.add_argument('--timestamp-field', type=str, default='', help='Column to use as the timestamp field')
    parser.add_argument('--index-name', type=str, default='', help='Name for the index. Must be all lowercase')
    parser.add_argument('--time-format-string', '-t', default='', help='Format string for the timestamp field')

    args = parser.parse_args()

    # Prompt user if necessary
    if not args.csv:
        csv_location = input("Please enter the location of the CSV file(s): ")
    else:
        csv_location = args.csv

    if os.path.isdir(csv_location):
        csv_files = glob.glob(os.path.join(csv_location, "*.csv"))
        ## Check encoding of file
        try:
            df = pd.concat([pd.read_csv(f,encoding_errors='replace') for f in csv_files], ignore_index=True)
        except:
            print('File encoding not utf-8. Checking file encoding')
            # detect file encoding using chardet
            with open(csv_files[0], 'rb') as f:
                result = detect(f.read())

            file_encoding = result['encoding']
            df = pd.concat([pd.read_csv(f, encoding=file_encoding,encoding_errors='replace') for f in csv_files], ignore_index=True)
    else:
        try:
            df = pd.read_csv(csv_location,encoding_errors='replace')
        except:
            # detect file encoding using chardet
            with open(csv_location, 'rb') as f:
                result = detect(f.read())

            file_encoding = result['encoding']
            df = pd.read_csv(csv_location,encoding=file_encoding,encoding_errors='replace')

    if not args.json_field:
        ## Check for JSON data in dataframe using json loads
        json_cols = []

        ### Loop through each column and check if it contains JSON data
        for col in df.select_dtypes(include='object').columns:
            try:
                # Attempt to load the first value in the column as a JSON object
                json.loads(df[col].iloc[0])
                json_cols.append(col)
            except (TypeError, json.JSONDecodeError):
                pass

        if len(json_cols) > 0:
            json_normilze_col = input(f"Please enter the field with JSON data. It appears {', '.join(json_cols)} have proper json structured data: ")
        else:
            print('No columns found with JSON data. Moving on...')
    else:
        json_normilze_col = args.json_field

    # Use pd.json_normalize() to expand the JSON data in each column
    print("Expanding JSON data into new columns\n")
    col = json_normilze_col
    df = pd.concat([df, pd.json_normalize(df[col].apply(json.loads),)], axis=1)
    # check for duplicate column names and rename if necessary
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
    df.columns = cols

    print("JSON data expanded successfully\n")

    if not args.timestamp_field:
        time_cols = [col for col in df.columns if re.search('time|date' ,col.lower())]
        print("Selecting Timestamp column\nHere are the columns with time oriented names:")
        for col in time_cols:
            print(col)

        timestamp_field = input(f"Please choose a column to use as the timestamp field: ")
    else:
        timestamp_field = args.timestamp_field

    # Timestamp Field in pandas
    # Display column names to user and prompt for timestamp_field

    if not args.time_format_string:
        # Display first value in timestamp column and prompt user for format string
        print(f"The first value in {timestamp_field} is {df[timestamp_field].iloc[0]}.")
        while True:
            time_format_string = input("Please enter the format string for the timestamp (e.g. '%Y-%m-%d %H:%M:%S'): ")
            try:
                pd.to_datetime(df[timestamp_field], format=time_format_string)
                break
            except ValueError:
                print(f"{time_format_string} does not match the pandas datetime format string. Please try again.")
                
    else:
        time_format_string = args.time_format_string
        while True:
            try:
                pd.to_datetime(df[timestamp_field], format=time_format_string)
                break
            except ValueError:
                print(f"{time_format_string} does not match the pandas datetime format string. Please try again.\n")
                time_format_string = input("Please enter the format string for the timestamp (e.g. '%Y-%m-%d %H:%M:%S'): ")

    df[timestamp_field] = pd.to_datetime(df[timestamp_field], format=time_format_string,errors='ignore',)
    df[timestamp_field] = df[timestamp_field].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    #Setup ES Index
    print("Setting up transport to elastic index. Ensure connection to ELK in place over port 9200")
    if not args.index_name:
        existing_indices = es_client.cat.indices(index='*',h='index',).split()
        existing_indices = [indice for indice in existing_indices if not indice.startswith('.') and not indice.startswith('elastalert_')]

        index_option = input(f"Please choose an existing index ({', '.join(existing_indices)}) or enter a new name for the index: ")
        if index_option in existing_indices:
            index_name = index_option
        else:
            index_name = index_option.strip().lower()
    else:
        index_name = (args.index_name).lower()

    # Define generator function to create documents for indexing
    def doc_generator(records, timestamp_field):
        for i, record in enumerate(records):
            doc = {
                "_index": index_name,
                "_id": i,
                "_source": {
                    "Timestamp": record[timestamp_field],
                    **record
                }
            }
            yield doc

    # Drop duplicates and convert to JSON records
    print("Converting to json records for upload....This might take a min....\n")
    pldf = pl.from_pandas(df.sort_values(timestamp_field).dropna(subset=timestamp_field).astype(pd.StringDtype()).drop_duplicates())
    json_records = [{k:v for k,v in row.items() if v is not None} for row in pldf.iter_rows(named=True)]

    print(f"Conversion Complete. {len(df)} rows converted. Ready to send to Index\n")



    # Create index and mapping if it does not exist.
    if not es_client.indices.exists(index=index_name):
        mapping = {
            "mappings": {
                "properties": {
                    "Timestamp": {
                        "type": "date"
                    }
                }
            }
        }
        es_client.indices.create(index=index_name, body=mapping)
    print("Starting upload...")
    with tqdm(total=len(json_records), desc="Indexing progress") as pbar:
        for ok, response in helpers.streaming_bulk(es_client, doc_generator(records=json_records, timestamp_field=timestamp_field), chunk_size=1000):
            if not ok:
                print(response)

            # Update progress bar
            pbar.update(1)

    print(f"Indexing Complete. {len(json_records)} sent to index {index_name} ")

if __name__ == "__main__":
    index_csv_files()

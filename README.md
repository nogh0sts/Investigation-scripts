# Investigation-scripts
A collection of python and powershell scripts I use to extract and analyze host based artifacts

CSV to Elasticsearch Indexing Tool - csv_to_es.py
---
Have you ever found yourself in a situation where you have a time-oriented CSV dataset that you want to start sorting and searching, but you're struggling with SOF-ELK and logstash? Fear not! This Python script can help.

Introduction
This tool is designed to help cyber security analysts who are struggling getting csv data into SOF-ELK that's not preconfigured for Logstash. It takes a time-oriented CSV dataset and indexes it into Elasticsearch for easy searching and sorting.

The tool uses the following libraries:

Pandas
Elasticsearch
Charset_normalizer
Tqdm
Argparse
Polars 

*Pre-Requisite*
Data is sent via the Elasticsearch API bulk upload endpoint. The ES API is only is only available over port 9200 from the localhost (without changing ES config files). 
*Tip*
Using a local port forward with SSH is quick workaround to manually editing the ES network configurations to allow remote api connections (which for me, has been a huge pain)
Example with [SOF-ELK](https://github.com/philhagen/sof-elk) - `ssh -L 9200:localhost:9200 elk_user@sof-elk`

How to Use
Install the required libraries using pip install pip install pandas elasticsearch charset_normalizer tqdm argparse polars
Run the script using python csv_to_es.py.
The script will prompt you to enter the location of the CSV file(s).
If the CSV file(s) are located in a folder, the script will detect all CSV files and check for their encoding.
If the CSV file(s) are located in a single file, the script will also detect its encoding.
You will then be prompted to specify which column contains JSON data (if any).
You will then be prompted to specify which column to use as the timestamp field.
If the timestamp field is not already in Pandas datetime format, you will be prompted to enter the format string for the timestamp.
Finally, you will be prompted to choose an existing Elasticsearch index or create a new one.

Conclusion
This Python script is a helpful tool for cyber security analysts who are struggling with SOF-ELK and logstash. It makes indexing time-oriented CSV datasets into Elasticsearch for easy searching and sorting a breeze. Try it out today and see how much easier your investigations become!
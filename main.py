from azure.data.tables import TableClient
import configparser
import yaml
import psycopg2
from psycopg2.extras import NamedTupleCursor, RealDictCursor
from psycopg2.extensions import cursor, connection
import csv
import os
import logging
import configparser
import json
import polars as pl
import uuid
from azure_table import PipelineSourceRepository
from upload import upload_dataframe_to_blob

logging.basicConfig(level=logging.INFO, filename='data_extraction.log')


def connect_to_database(config: dict):
    try:
        return psycopg2.connect(**config)
    except psycopg2.Error as e:
        logging.error(f"Database connection failed: {e}")
        raise

def main(config_file):
    logging.info('starting extraction')
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
        
    # Read configuration
    parser = configparser.ConfigParser()
    parser.read('pipeline.conf')
    connection_string = parser.get('azure_config', 'connection_string')
    storage_table_name = parser.get('azure_config', 'table_name')
        
    # Create repository instance
    repo = PipelineSourceRepository(connection_string, storage_table_name)

    for extractor in config['extractors']:
        with connect_to_database(extractor['config']) as conn:
            source_name = extractor['name']
            
            for table in extractor['tables']:
                table_name = table['name']
                method = table['method']
                repo.create_entity(source_name, table_name, {'ExtractMethod': method})
                
                # Generate Query
                select_columns = table['select']
                if isinstance(select_columns, list):
                    select_columns = ", ".join([f'"{col}"' for col in select_columns])
                
                query = f"SELECT {select_columns} FROM {table_name}"
                
                if method == 'incremental':
                    latest_value = repo.get_latest_updated_value(source_name, table_name)
                    query += f" WHERE {table['incremental_column']} > '{latest_value}'"
                
                # Execute query
                df = pl.read_database(query, conn)
                if df.is_empty():
                    logging.info('No data to extract')
                    continue
                
                # Write extracted data to landing
                blob_name = f"{extractor['name']}/{table_name.replace('.', '/')}/{uuid.uuid4()}.csv"
                upload_dataframe_to_blob(
                    df=df,
                    account_name='joshstorageforfun',
                    account_key='',
                    container_name='landing',
                    blob_name=blob_name,
                    file_format='csv'
                )
                
                # If incremental, update the timestamp in the cache table
                if method == 'incremental':
                    last_updated_value = str(df.select(table['incremental_column']).to_series().max())
                    repo.update_entity(
                        partition_key=source_name,
                        row_key=table_name,
                        properties={
                            'LatestUpdatedValue': last_updated_value,
                            'IncrementalKey': table['incremental_column']
                        }
                    )

if __name__ == "__main__":
    main('sources/adventure_works.yaml')
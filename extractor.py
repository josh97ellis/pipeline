from abc import ABC, abstractmethod
import polars as pl
import requests


class DataExtractor(ABC):
    @abstractmethod
    def fetch_data(self, table_name):
        pass


class SQLDataExtractor(DataExtractor):
    def __init__(self, connection_details):
        self.connection = connection_details

    def fetch_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, self.connection)


class APIDataExtractor(DataExtractor):
    def __init__(self, api_details):
        self.api_details = api_details

    def fetch_data(self, resource):
        response = requests.get(f"{self.api_details['base_url']}/{resource}")
        return pd.DataFrame(response.json())

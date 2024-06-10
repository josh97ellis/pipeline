from azure.data.tables import TableClient, UpdateMode
from azure.core.exceptions import ResourceExistsError, HttpResponseError


class PipelineSourceRepository:
    def __init__(self, connection_string, table_name):
        self.connection_string = connection_string
        self.table_name = table_name

    def create_entity(self, partition_key: str, row_key: str, additional_properties=None):
        additional_properties = additional_properties or {}
        entity = {'PartitionKey': partition_key, 'RowKey': row_key, **additional_properties}
        with TableClient.from_connection_string(self.connection_string, self.table_name) as client:
            try:
                client.create_entity(entity=entity)
            except ResourceExistsError:
                pass  # Entity already exists, could log this if needed

    def update_entity(self, partition_key: str, row_key: str, properties):
        with TableClient.from_connection_string(self.connection_string, self.table_name) as client:
            try:
                entity = {'PartitionKey': partition_key, 'RowKey': row_key, **properties}
                client.update_entity(entity=entity, mode=UpdateMode.MERGE)
            except HttpResponseError as e:
                print(f"Failed to update entity: {e}")

    def get_latest_updated_value(self, partition_key: str, row_key: str):
        with TableClient.from_connection_string(self.connection_string, self.table_name) as client:
            try:
                entity = client.get_entity(partition_key, row_key)
                return entity.get('LatestUpdatedValue', '1970-01-01')  # Provide default if not found
            except KeyError:
                return '1970-01-01'  # Default if the entity doesn't exist

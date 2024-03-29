import os
from datetime import datetime

from jinja2 import Environment, FileSystemLoader


class OffenderExternalMovements:
    """Produces JSON payloads for offender_external_movements"""

    def __init__(self, template_dir, timestamp_format: str = '%Y-%m-%dT%H:%M:%S.%fZ') -> None:
        table_name = 'offender_external_movements'
        self.__jinja_env = Environment(loader=FileSystemLoader(os.path.join(template_dir, table_name)))
        self.__timestamp_format = timestamp_format
        self.__create_table_template = self.__jinja_env.get_template('create-table.json')
        self.__load_table_template = self.__jinja_env.get_template('load.json')
        self.__cdc_template = self.__jinja_env.get_template('cdc.json')

    def create_table_payload(self, timestamp: datetime) -> str:
        return self.__create_table_template.render(timestamp=timestamp.strftime(self.__timestamp_format))

    def load_payload(self, timestamp: datetime, offender_id: int, movement_seq: int) -> str:
        return self.__load_table_template.render(timestamp=timestamp.strftime(self.__timestamp_format),
                                                 offender_id=offender_id,
                                                 movement_seq=movement_seq)

    def update_payload(self, timestamp: datetime, offender_id: int, movement_seq: int, transaction_id: int) -> str:
        return self.__cdc_template.render(timestamp=timestamp, offender_id=offender_id, movement_seq=movement_seq,
                                          transaction_id=transaction_id, operation='update')

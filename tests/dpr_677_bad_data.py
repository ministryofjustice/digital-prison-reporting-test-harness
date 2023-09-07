import json
import logging
from datetime import datetime, timedelta

from faker import Faker

from utils.Config import get_config
from utils.Kinesis import Kinesis
from utils.payloads.OffenderExternalMovements import OffenderExternalMovements

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('Writing data for DPR-677 (UDF problems on null JSON fields)')
    # Send a stream of semi realistic messages,
    # with some missing TO_ADDRESS_ID and/or FROM_ADDRESS_ID to reproduce the issue in DPR-677

    profile_name = get_config('aws', 'profile')
    stream = Kinesis(get_config('kinesis', 'stream'))

    offender_external_movements = OffenderExternalMovements(template_dir='../payloads/templates')

    faker = Faker(locale="en_GB")
    offender_id = faker.random_int(min=1000, max=99999)
    movement_seq = faker.random_int(min=1, max=999999)

    create_table_time = datetime.now() - timedelta(hours=1)

    create_table_payload = offender_external_movements.create_table_payload(timestamp=create_table_time)
    send_result = stream.send_stream(data=create_table_payload, profile_name=profile_name)
    logging.info("Sent create table message for table :: offender_external_movements {}".format(send_result))

    load_time = create_table_time + timedelta(seconds=3)
    load_payload = offender_external_movements.load_payload(timestamp=load_time, offender_id=offender_id,
                                                            movement_seq=movement_seq)
    send_result = stream.send_stream(data=load_payload, profile_name=profile_name)
    logging.info("Sent load message for table :: offender_external_movements {}".format(send_result))

    cdc_time = load_time + timedelta(minutes=3)
    transaction_id = 10
    num_messages = 100
    for i in range(num_messages):
        movement_seq = movement_seq + 1
        cdc_payload = offender_external_movements.update_payload(timestamp=cdc_time, offender_id=offender_id,
                                                                 movement_seq=movement_seq,
                                                                 transaction_id=transaction_id)
        if i % 3 == 0:
            # Every 3rd message remove TO_ADDRESS_ID
            json_dict = json.loads(cdc_payload)
            del json_dict['data']['TO_ADDRESS_ID']
            cdc_payload = json.dumps(json_dict, indent=2)
        if i % 5 == 0:
            # Every 5th message remove FROM_ADDRESS_ID
            json_dict = json.loads(cdc_payload)
            del json_dict['data']['FROM_ADDRESS_ID']
            cdc_payload = json.dumps(json_dict, indent=2)
        partition_key = 'OMS_OWNER.OFFENDER_EXTERNAL_MOVEMENTS.{}'.format(transaction_id)
        send_result = stream.send_stream(data=cdc_payload, profile_name=profile_name, partition_key=partition_key)
        logging.info("Sent CDC message for table :: offender_external_movements {}".format(send_result))
        cdc_time + timedelta(minutes=3)
        transaction_id = transaction_id + 1

    logging.info('Finished writing data for DPR-677 (UDF problems on null JSON fields)')

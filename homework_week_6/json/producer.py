import csv
import json
from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from ride import Ride_fhv, Ride_green
import settings as st


class JsonProducer(KafkaProducer):
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_records(resource_path: str):
        records = []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header row
            if 'fhv' in resource_path:
                for row in reader:
                    records.append(Ride_fhv(arr=row))
            elif 'green' in resource_path:
                for row in reader:
                    records.append(Ride_green(arr=row))
            else:
                print('no class defined for import file')                
        return records


    def publish_rides(self, topic: str, messages: List):
        if topic == st.KAFKA_TOPIC_FHV:
            for ride in messages:
                try:
                    record = self.producer.send(topic=topic, key=ride.PUlocationID, value=ride)
                    print('For Record {} successfully produced at offset {}'.format(ride.PUlocationID, record.get().offset))
                except KafkaTimeoutError as e:
                    print(e.__str__())
            print(f'kafa topic is: {topic}')
        if topic == st.KAFKA_TOPIC_GREEN:
            for ride in messages:
                try:
                    record = self.producer.send(topic=topic, key=ride.PULocationID, value=ride)
                    print('For Record {} successfully produced at offset {}'.format(ride.PULocationID, record.get().offset))
                except KafkaTimeoutError as e:
                    print(e.__str__())
            print(f'kafa topic is: {topic}')
        else:
            print('no kafka topic defined in settings file')



if __name__ == '__main__':
    # Config Should match with the KafkaProducer expectation
    config = {
        'bootstrap_servers': st.BOOTSTRAP_SERVERS,
        'key_serializer': lambda key: str(key).encode(),
        'value_serializer': lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')
    }
    producer = JsonProducer(props=config)
    rides_fhv = producer.read_records(resource_path=st.INPUT_DATA_PATH_FHV)
    rides_green = producer.read_records(resource_path=st.INPUT_DATA_PATH_GREEN)
    producer.publish_rides(topic=st.KAFKA_TOPIC_FHV, messages=rides_fhv)
    producer.publish_rides(topic=st.KAFKA_TOPIC_GREEN, messages=rides_green)
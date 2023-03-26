from typing import List, Dict
from decimal import Decimal
from datetime import datetime


class Ride_fhv:
    def __init__(self, arr: List[str]):
        self.dispatching_base_num = arr[0]
        self.pickup_datetime = datetime.strptime(arr[1], "%Y-%m-%d %H:%M:%S"),
        self.dropOff_datetime = datetime.strptime(arr[2], "%Y-%m-%d %H:%M:%S"),
        self.PUlocationID = int(arr[3])
        self.DOlocationID = int(arr[4])
        self.SR_Flag = (arr[5])
        self.Affiliated_base_number = (arr[6])


    @classmethod
    def from_dict(cls, d: Dict):
        return cls(arr=[
            d['dispatching_base_num'],
            d['pickup_datetime'][0],
            d['dropOff_datetime'][0],
            d['PUlocationID'],
            d['DOlocationID'],
            d['SR_Flag'],
            d['Affiliated_base_number'],
            
        ]
        )

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'
    

class Ride_green:
    def __init__(self, arr: List[str]):
        self.VendorID = arr[0]
        self.lpep_pickup_datetime = datetime.strptime(arr[1], "%Y-%m-%d %H:%M:%S"),
        self.lpep_dropoff_datetime = datetime.strptime(arr[2], "%Y-%m-%d %H:%M:%S"),
        self.store_and_fwd_flag = arr[3]
        self.RatecodeID = int(arr[4])
        self.PULocationID = int(arr[5])
        self.DOLocationID = int(arr[6])
        self.passenger_count = int(arr[7])
        self.trip_distance = Decimal(arr[8])
        self.fare_amount = Decimal(arr[9])
        self.extra = Decimal(arr[10])
        self.mta_tax = Decimal(arr[11])
        self.tip_amount = Decimal(arr[12])
        self.tolls_amount = Decimal(arr[13])
        self.ehail_fee = Decimal(arr[14])
        self.improvement_surcharge = Decimal(arr[15])
        self.total_amount = Decimal(arr[16])
        self.payment_type = arr[17]
        self.trip_type = arr[18]
        self.congestion_surcharge = Decimal(arr[19])

    @classmethod
    def from_dict(cls, d: Dict):
        return cls(arr=[
            d['VendorID'],
            d['tpep_pickup_datetime'][0],
            d['tpep_dropoff_datetime'][0],
            d['store_and_fwd_flag'],
            d['RatecodeID'],
            d['PULocationID'],
            d['DOLocationID'],
            d['passenger_count'],
            d['trip_distance'],
            d['fare_amount'],
            d['extra'],
            d['mta_tax'],
            d['tip_amount'],
            d['tolls_amount'],
            d['ehail_fee'],
            d['improvement_surcharge'],
            d['total_amount'],
            d['payment_type'],
            d['trip_type'],
            d['congestion_surcharge'],
        ]
        )

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'


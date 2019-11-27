import boto3
import datetime
import time
import json
import threading
import random

REGIONS = {
    "NORTH": ["AM", "RR", "PA", "TO", "AC", "RO", "MA", "AP"],
    "NORTHEAST": ["MA", "BA", "AL", "PE", "RN", "CE", "PB", "SE", "PI"],
    "SOUTH": ["RS", "PR", "SC"],
    "SOUTHEAST": ["MG", "SP", "RJ", "ES"],
    "MIDWEST": [ "MT", "GO", "MS"]
}

if __name__ == "__main__":
    kin_cli = boto3.client("kinesis")

    kinesis_stream_name = "kds_dashboard_input_stream"
    
    cnt = 0

    for j in range(1, 100000):
        kinesis_records = []
        for i in range(1, 100):

            timestamp = datetime.datetime.utcnow()
            part_key = "8.8.8.8"

            region = random.choice(list(REGIONS.keys())) 
            state = random.choice(REGIONS[region])
            
            data = {
                "event-time" : timestamp.isoformat(),
                "state": state,
                "region": region,
                "store-id": random.randint(1, 50),
                "kpi-1": random.randint(1, 100),
                "kpi-2": random.randint(1, 100),
                "kpi-3": random.randint(1, 100),
                "kpi-4": random.randint(1, 100),
                "kpi-5": random.randint(1, 100),
                "kpi-6": random.randint(1, 100),
                "kpi-7": random.randint(1, 100)
            }

            if i % 20 == 0:
                print("{} - {} - SAMPLE: {}".format(j, i, json.dumps(data)))
                
            record = { 'PartitionKey': part_key, 'Data': json.dumps(data) }
            kinesis_records.append(record)

        response = kin_cli.put_records(
            Records=kinesis_records,
            StreamName = kinesis_stream_name
        )

        cnt += len(kinesis_records)

        wait = random.uniform(0, 5)
        print("{} - {} -       WAIT: {}".format(j, i, wait))
        print("{} - {} - TOTAL SENT: {}".format(j, i, cnt))
        time.sleep(wait)


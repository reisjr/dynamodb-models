import boto3
import datetime
import time
import json
import threading
import random
import pprint

REGIONS = {
    "NORTH": ["AM", "RR", "PA", "TO", "AC", "RO", "MA", "AP"],
    "NORTHEAST": ["MA", "BA", "AL", "PE", "RN", "CE", "PB", "SE", "PI"],
    "SOUTH": ["RS", "PR", "SC"],
    "SOUTHEAST": ["MG", "SP", "RJ", "ES"],
    "MIDWEST": [ "MT", "GO", "MS"]
}

REGIONS_RND = {}

pp = pprint.PrettyPrinter(indent=4)

for r_gens in REGIONS.keys():
    REGIONS_RND[r_gens] = random.Random()


if __name__ == "__main__":
    kin_cli = boto3.client("kinesis")

    kinesis_stream_name = "kds_dashboard_input_stream"
    
    cnt = 0

    for j in range(1, 100000):
        kinesis_records = []
        for i in range(1, 100):

            timestamp = datetime.datetime.utcnow()
            
            region = random.choice(list(REGIONS.keys())) 
            state = random.choice(REGIONS[region])
            
            part_key = "{}#{}".format(region, state)
            
            rnd = REGIONS_RND.get(region)

            data = {
                "event-time" : timestamp.isoformat(),
                "state": state,
                "region": region,
                "store-id": rnd.randint(1, 15),
                "kpi-1": rnd.randint(0, 250),
                "kpi-2": rnd.randint(0, 50),
                "kpi-3": int(rnd.gauss(50.0, 15)),
                "kpi-4": int(rnd.gauss(30.0, 5)),
                "kpi-5": int(rnd.gauss(100.0, 40)),
                "kpi-6": int(rnd.uniform(5, 100)),
                "kpi-7": rnd.randint(1, 35)
            }

            if i % 20 == 0:
                #print("{} - {} - SAMPLE: {}".format(j, i, json.dumps(data)))
                print("{} - {} - SAMPLE: {}".format(j, i, "" ))
                pp.pprint(data) 
                
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


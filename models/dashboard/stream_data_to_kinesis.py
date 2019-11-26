import boto3
import datetime
import time
import json
import threading
import random

'''

CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" ("example" VARCHAR(4), "region" VARCHAR(10), kpi_1_sum INTEGER,  kpi_2_sum INTEGER, ingest_time TIMESTAMP);

CREATE OR REPLACE  PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"

SELECT STREAM "example", "region", SUM("kpi-1") AS kpi_1_sum, SUM("kpi-2") AS kpi_2_sum, FLOOR("SOURCE_SQL_STREAM_001".APPROXIMATE_ARRIVAL_TIME TO MINUTE) as ingest_time
FROM "SOURCE_SQL_STREAM_001"
GROUP BY "example", "region", FLOOR("SOURCE_SQL_STREAM_001".APPROXIMATE_ARRIVAL_TIME TO MINUTE), FLOOR(("SOURCE_SQL_STREAM_001".ROWTIME - TIMESTAMP '1970-01-01 00:00:00') SECOND / 10 TO SECOND);

---

CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" ("example" VARCHAR(4), "region" VARCHAR(8), "store-id" VARCHAR(4), example_count INTEGER);

CREATE OR REPLACE  PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"

SELECT STREAM "example", "region", "store-id", COUNT(*) AS example_count
FROM "SOURCE_SQL_STREAM_001"
GROUP BY "example", "region", "store-id", FLOOR(("SOURCE_SQL_STREAM_001".ROWTIME - TIMESTAMP '1970-01-01 00:00:00') SECOND / 10 TO SECOND);
---

CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" ("example" VARCHAR(4), "region" VARCHAR(10), kpi_1_sum INTEGER,  kpi_2_sum INTEGER);

CREATE OR REPLACE  PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"

SELECT STREAM "example", "region", SUM("kpi-1") AS kpi_1_sum, SUM("kpi-2") AS kpi_2_sum
FROM "SOURCE_SQL_STREAM_001"
GROUP BY "example", "region", FLOOR(("SOURCE_SQL_STREAM_001".ROWTIME - TIMESTAMP '1970-01-01 00:00:00') SECOND / 10 TO SECOND);
'''

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

    for j in range(1, 1000):
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
                "store-id": random.randint(1, 10),
                "kpi-1": random.randint(1, 100),
                "kpi-2": random.randint(1, 100),
                "kpi-3": random.randint(1, 100),
                "kpi-4": random.randint(1, 100),
                "kpi-5": random.randint(1, 100),
                "kpi-6": random.randint(1, 100),
                "kpi-7": random.randint(1, 100)
            }

            if i % 20 == 0:
                wait = random.uniform(0, 20)
                print("{} - {} - wait: {}".format(j, i, wait))
                print("SAMPLE:\n{}".format(json.dumps(data)))
                time.sleep(wait)

            record = { 'PartitionKey': part_key, 'Data': json.dumps(data) }
            kinesis_records.append(record)

        response = kin_cli.put_records(
            Records=kinesis_records,
            StreamName = kinesis_stream_name
        )

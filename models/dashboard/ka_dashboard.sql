CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM_BY_STORE" (
    "type" VARCHAR(14), "region" VARCHAR(10), 
    "state" VARCHAR(2), "store-id" INTEGER, 
    kpi_1_sum INTEGER, kpi_2_sum INTEGER, 
    kpi_3_sum INTEGER, kpi_4_sum INTEGER, 
    kpi_5_sum INTEGER,
    n_rec INTEGER,
    ingest_time TIMESTAMP,
    event_time TIMESTAMP);

-- PT1M

CREATE OR REPLACE PUMP "STREAM_PUMP_REGION_PT1M" AS INSERT INTO "DESTINATION_SQL_STREAM_BY_STORE"
SELECT STREAM UPPER('BY_REGION_PT1M'), "region", UPPER('NN'), -1, 
    SUM("kpi_1") AS kpi_1_sum, SUM("kpi_2") AS kpi_2_sum, 
    SUM("kpi_3") AS kpi_3_sum, SUM("kpi_4") AS kpi_4_sum, 
    SUM("kpi_5") AS kpi_5_sum, 
    COUNT(*) as n_rec,
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '60' SECOND) as ingest_time,
    STEP("event_time" BY INTERVAL '60' SECOND) as event_time
FROM "SOURCE_SQL_STREAM_001"
GROUP BY UPPER('BY_REGION_PT1M'), "region",
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '60' SECOND),
    STEP("event_time" BY INTERVAL '60' SECOND);
    
CREATE OR REPLACE PUMP "STREAM_PUMP_STATE_PT1M" AS INSERT INTO "DESTINATION_SQL_STREAM_BY_STORE"
SELECT STREAM UPPER('BY_STATE_PT1M'), "region", "state", -1, 
    SUM("kpi_1") AS kpi_1_sum, SUM("kpi_2") AS kpi_2_sum, 
    SUM("kpi_3") AS kpi_3_sum, SUM("kpi_4") AS kpi_4_sum, 
    SUM("kpi_5") AS kpi_5_sum, 
    COUNT(*) as n_rec,
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '60' SECOND) as ingest_time,
    STEP("event_time" BY INTERVAL '60' SECOND) as event_time
FROM "SOURCE_SQL_STREAM_001"
GROUP BY UPPER('BY_STATE_PT1M'), "region", "state",
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '60' SECOND),
    STEP("event_time" BY INTERVAL '60' SECOND);

CREATE OR REPLACE PUMP "STREAM_PUMP_STORE_PT1M" AS INSERT INTO "DESTINATION_SQL_STREAM_BY_STORE"
SELECT STREAM UPPER('BY_STORE_PT1M'), "region", "state", "store_id", 
    SUM("kpi_1") AS kpi_1_sum, SUM("kpi_2") AS kpi_2_sum, 
    SUM("kpi_3") AS kpi_3_sum, SUM("kpi_4") AS kpi_4_sum, 
    SUM("kpi_5") AS kpi_5_sum,
    COUNT(*) as n_rec,
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '60' SECOND) as ingest_time,
    STEP("event_time" BY INTERVAL '60' SECOND) as event_time
FROM "SOURCE_SQL_STREAM_001"
GROUP BY UPPER('BY_STORE_PT1M'), "region", "state", "store_id", 
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '60' SECOND),
    STEP("event_time" BY INTERVAL '60' SECOND);

-- PT10M

CREATE OR REPLACE PUMP "STREAM_PUMP_REGION_PT10M" AS INSERT INTO "DESTINATION_SQL_STREAM_BY_STORE"
SELECT STREAM UPPER('BY_REGION_PT10M'), "region", UPPER('NN'), 0, 
    SUM("kpi_1") AS kpi_1_sum, SUM("kpi_2") AS kpi_2_sum, 
    SUM("kpi_3") AS kpi_3_sum, SUM("kpi_4") AS kpi_4_sum, 
    SUM("kpi_5") AS kpi_5_sum,
    COUNT(*) as n_rec,
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '10' MINUTE) as ingest_time,
    STEP("event_time" BY INTERVAL '10' MINUTE) as event_time
FROM "SOURCE_SQL_STREAM_001"
GROUP BY UPPER('BY_REGION_PT10M'), "region",
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '10' MINUTE), 
    STEP("event_time" BY INTERVAL '10' MINUTE);

CREATE OR REPLACE PUMP "STREAM_PUMP_STATE_PT10M" AS INSERT INTO "DESTINATION_SQL_STREAM_BY_STORE"
SELECT STREAM UPPER('BY_STATE_PT10M'), "region", "state", -1, 
    SUM("kpi_1") AS kpi_1_sum, SUM("kpi_2") AS kpi_2_sum, 
    SUM("kpi_3") AS kpi_3_sum, SUM("kpi_4") AS kpi_4_sum, 
    SUM("kpi_5") AS kpi_5_sum, 
    COUNT(*) as n_rec,
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '10' MINUTE) as ingest_time,
    STEP("event_time" BY INTERVAL '10' MINUTE) as event_time
FROM "SOURCE_SQL_STREAM_001"
GROUP BY UPPER('BY_STATE_PT10M'), "region", "state",
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '10' MINUTE), 
    STEP("event_time" BY INTERVAL '10' MINUTE);

CREATE OR REPLACE PUMP "STREAM_PUMP_STORE_PT10M" AS INSERT INTO "DESTINATION_SQL_STREAM_BY_STORE"
SELECT STREAM UPPER('BY_STORE_PT10M'), "region", "state", "store_id", 
    SUM("kpi_1") AS kpi_1_sum, SUM("kpi_2") AS kpi_2_sum, 
    SUM("kpi_3") AS kpi_3_sum, SUM("kpi_4") AS kpi_4_sum, 
    SUM("kpi_5") AS kpi_5_sum,
    COUNT(*) as n_rec,
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '10' MINUTE) as ingest_time,
    STEP("event_time" BY INTERVAL '10' MINUTE) as event_time
FROM "SOURCE_SQL_STREAM_001"
GROUP BY UPPER('BY_STORE_PT10M'), "region", "state", "store_id", 
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '10' MINUTE), 
    STEP("event_time" BY INTERVAL '10' MINUTE);

-- PT1H

CREATE OR REPLACE PUMP "STREAM_PUMP_REGION_PT1H" AS INSERT INTO "DESTINATION_SQL_STREAM_BY_STORE"
SELECT STREAM UPPER('BY_REGION_PT1H'), "region", UPPER('NN'), 0, 
    SUM("kpi_1") AS kpi_1_sum, SUM("kpi_2") AS kpi_2_sum, 
    SUM("kpi_3") AS kpi_3_sum, SUM("kpi_4") AS kpi_4_sum, 
    SUM("kpi_5") AS kpi_5_sum, 
    COUNT(*) as n_rec,
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '1' HOUR) as ingest_time,
    STEP("event_time" BY INTERVAL '1' HOUR) as event_time
FROM "SOURCE_SQL_STREAM_001"
GROUP BY UPPER('BY_REGION_PT1H'), "region",
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '1' HOUR), 
    STEP("event_time" BY INTERVAL '1' HOUR);

CREATE OR REPLACE PUMP "STREAM_PUMP_STATE_PT1H" AS INSERT INTO "DESTINATION_SQL_STREAM_BY_STORE"
SELECT STREAM UPPER('BY_STATE_PT1H'), "region", "state", -1, 
    SUM("kpi_1") AS kpi_1_sum, SUM("kpi_2") AS kpi_2_sum, 
    SUM("kpi_3") AS kpi_3_sum, SUM("kpi_4") AS kpi_4_sum, 
    SUM("kpi_5") AS kpi_5_sum,
    COUNT(*) as n_rec,
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '1' HOUR) as ingest_time,
    STEP("event_time" BY INTERVAL '1' HOUR) as event_time
FROM "SOURCE_SQL_STREAM_001"
GROUP BY UPPER('BY_STATE_PT1H'), "region", "state",
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '1' HOUR), 
    STEP("event_time" BY INTERVAL '1' HOUR);

CREATE OR REPLACE PUMP "STREAM_PUMP_STORE_PT1H" AS INSERT INTO "DESTINATION_SQL_STREAM_BY_STORE"
SELECT STREAM UPPER('BY_STORE_PT1H'), "region", "state", "store_id", 
    SUM("kpi_1") AS kpi_1_sum, SUM("kpi_2") AS kpi_2_sum, 
    SUM("kpi_3") AS kpi_3_sum, SUM("kpi_4") AS kpi_4_sum, 
    SUM("kpi_5") AS kpi_5_sum,
    COUNT(*) as n_rec,
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '1' HOUR) as ingest_time,
    STEP("event_time" BY INTERVAL '1' HOUR) as event_time
FROM "SOURCE_SQL_STREAM_001"
GROUP BY UPPER('BY_STORE_PT1H'), "region", "state", "store_id", 
    STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '1' HOUR), 
    STEP("event_time" BY INTERVAL '1' HOUR);

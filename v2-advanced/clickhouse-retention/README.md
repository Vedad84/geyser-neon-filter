## Clickhouse retention utility
This service utility transfers data from update_account table to older_account table. Data is transfered by partiion. Each partition of the update_account table contains 216000 slots. It corresponds to one day. So, each partition corresponds to one day. 
After copying utility drops partition of the update_account table. Partions for copying and dropind are defined by config.yaml. 

**config.yaml:**
````
clickhouse_url: ["http://localhost:8123"]
copy_partition_offset: 3
drop_partition_offset: 10
log_file_name: "/var/log/neon/retention"
slot_range_to_task: 24000
tasks_at_same_time: 3
````
*copy_partition_offset* - count days relative to the current day for the copiyng. If you want to copy yesterday's data, you should set the value to 1. Recomended value - 30.

*drop_partition_offset* - the same, only for droppping partition. This value should be greater than copy_partition_offset.

*slot_range_to_task*  - the nubmer of slots in one data copying task

*tasks_at_same_time* - the number of simultaneously executed tasks

The utility works with config file or environmemt variables. The config file has a higher prioryty.
Set of variables:
````
export NEON_DB_CLICKHOUSE_URLS="http://localhost:8123"
export COPY_PARTITION_OFFSET=3
export DROP_PARTITION_OFFSET=10
export LOG_FILE_NAME="/var/log/neon/retention"
export SLOT_RANGE_TO_TASK=24000
export TASKS_AT_SAME_TIME=3
````

Performance: depends on count of rows in partition.

Example of execution:
```asm
clickhouse-retention -c config.yaml 
2023-06-16 13:51:34.253081579 INFO clickhouse_retention - Start copying to older_account
2023-06-16 13:51:39.018303734 INFO clickhouse_retention::client - partition to copy: "1030"
2023-06-16 13:52:21.744795143 INFO clickhouse_retention::client - 11395860 rows should be copied
2023-06-16 13:52:21.744808124 INFO clickhouse_retention::client - slots to copy: 222480000..222695999
2023-06-16 13:52:21.744811516 INFO clickhouse_retention::client - tasks at same time 3, slot range to task 24000
2023-06-16 13:52:21.744898887 INFO clickhouse_retention::client - slots 222480000..222504000 are being copied
2023-06-16 13:52:21.744992298 INFO clickhouse_retention::client - slots 222504000..222528000 are being copied
2023-06-16 13:52:21.745068089 INFO clickhouse_retention::client - slots 222528000..222552000 are being copied
2023-06-16 13:54:46.950741506 INFO clickhouse_retention::client - slots 222480000..222504000 done, task took 145.21 sec
2023-06-16 13:54:54.529937652 INFO clickhouse_retention::client - slots 222504000..222528000 done, task took 152.78 sec
2023-06-16 13:55:26.580156579 INFO clickhouse_retention::client - slots 222528000..222552000 done, task took 184.84 sec
2023-06-16 13:55:26.580393357 INFO clickhouse_retention::client - slots 222552000..222576000 are being copied
2023-06-16 13:55:26.580512506 INFO clickhouse_retention::client - slots 222576000..222600000 are being copied
2023-06-16 13:55:26.580632968 INFO clickhouse_retention::client - slots 222600000..222624000 are being copied
2023-06-16 13:57:53.914122194 INFO clickhouse_retention::client - slots 222600000..222624000 done, task took 147.33 sec
2023-06-16 13:58:27.843023586 INFO clickhouse_retention::client - slots 222576000..222600000 done, task took 181.26 sec
2023-06-16 13:58:34.521653184 INFO clickhouse_retention::client - slots 222552000..222576000 done, task took 187.94 sec
2023-06-16 13:58:34.521871937 INFO clickhouse_retention::client - slots 222624000..222648000 are being copied
2023-06-16 13:58:34.521985208 INFO clickhouse_retention::client - slots 222648000..222672000 are being copied
2023-06-16 13:58:34.522038532 INFO clickhouse_retention::client - slots 222672000..222696000 are being copied
2023-06-16 13:59:12.713218166 INFO clickhouse_retention::client - slots 222648000..222672000 done, task took 38.19 sec
2023-06-16 13:59:24.165571382 INFO clickhouse_retention::client - slots 222672000..222696000 done, task took 49.64 sec
2023-06-16 14:00:39.213315049 INFO clickhouse_retention::client - slots 222624000..222648000 done, task took 124.69 sec
2023-06-16 14:00:39.213457119 INFO clickhouse_retention::client - execution time: 544 sec
2023-06-16 14:00:39.213460063 INFO clickhouse_retention - Copying completed successfully
2023-06-16 14:00:39.213461291 INFO clickhouse_retention - Start to drop partition
2023-06-16 14:00:43.308848599 INFO clickhouse_retention::client - partition to drop: "1023"
2023-06-16 14:00:43.566648557 INFO clickhouse_retention - Partition has been dropped

```


-- compression and settings
SET mapred.output.compress=true;
SET hive.exec.compress.output=true; 
SET io.seqfile.compression.type=BLOCK;
 
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;


-- raw data
create external table kiss_raw (
    json_data string
)
location 's3://enter-kiss-test/revisions/' ;


create external table kiss (
    p string,    
    p2 string,
    dt string comment 'datetime',
    event string comment 'event',
    campaign_content string,
    campaign_medium string,
    campaign_name string,
    campaign_source string,
    campaign_terms string,
    search_engine string,
    search_terms string,
    json_data string comment 'original json'
)
partitioned by (`date` string)
stored as rcfile
location 's3://enter-kiss-test/hive/kiss/';
alter table kiss recover partitions;
 

create external table kiss_normalized (
    p string,    
    dt string comment 'datetime',
    event string comment 'event',
    campaign_content string,
    campaign_medium string,
    campaign_name string,
    campaign_source string,
    campaign_terms string,
    search_engine string,
    search_terms string,
    json_data string comment 'original json'
)
partitioned by (`date` string)
stored as rcfile
location 's3://enter-kiss-test/hive/kiss_normalized/';
alter table kiss_normalized recover partitions;
 

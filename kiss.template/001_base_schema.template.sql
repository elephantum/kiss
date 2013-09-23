create table kiss_{data_source} (
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
location 's3://enter-kiss-test/enter_proto_{data_source}/kiss_{data_source}/';
alter table kiss_{data_source} recover partitions;


create table session_pairs_{data_source}(
  p string, 
  p2 string)
location 's3://enter-kiss-test/enter_proto_{data_source}/session_pairs/';


create external table session_alias_{data_source} (
    session string, 
    alias string) 
location 's3://enter-kiss-test/enter_proto_{data_source}/sessions/';


create table kiss_normalized_{data_source} (
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
location 's3://enter-kiss-test/enter_proto_{data_source}/kiss_normalized_{data_source}/';
alter table kiss_normalized_{data_source} recover partitions;



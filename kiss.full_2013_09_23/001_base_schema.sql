create table kiss_full_2013_09_23 (
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
location 's3://enter-kiss-test/enter_proto_full_2013_09_23/kiss_full_2013_09_23/';
alter table kiss_full_2013_09_23 recover partitions;


create table session_pairs_full_2013_09_23(
  p string, 
  p2 string)
location 's3://enter-kiss-test/enter_proto_full_2013_09_23/session_pairs/';


create external table session_alias_full_2013_09_23 (
    session string, 
    alias string) 
location 's3://enter-kiss-test/enter_proto_full_2013_09_23/sessions/';


create table kiss_normalized_full_2013_09_23 (
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
location 's3://enter-kiss-test/enter_proto_full_2013_09_23/kiss_normalized_full_2013_09_23/';
alter table kiss_normalized_full_2013_09_23 recover partitions;



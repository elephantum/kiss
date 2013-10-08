create table kiss_full_2013_10_08 (
    p string,    
    p2 string,
    dt string comment 'datetime',
    event string comment 'event',
    json_data string comment 'original json'
)
partitioned by (year_month string)
stored as rcfile
location 's3://enter-kiss-test/enter_proto_full_2013_10_08/kiss_full_2013_10_08/';
alter table kiss_full_2013_10_08 recover partitions;


create table session_pairs_full_2013_10_08(
  p string, 
  p2 string)
location 's3://enter-kiss-test/enter_proto_full_2013_10_08/session_pairs/';


create external table session_alias_full_2013_10_08 (
    session string, 
    alias string) 
location 's3://enter-kiss-test/enter_proto_full_2013_10_08/sessions/';

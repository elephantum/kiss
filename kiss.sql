-- сырые данные
create external table kiss_raw (
    json_data string
)
location 's3://enter-kiss-test/revisions/' ;
 
create external table sessions_dict (
    session string, 
    alias string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY "," 
location 's3://enter-kiss-test/hive/sessions/';

-- компрессия
SET mapred.output.compress=true;
SET hive.exec.compress.output=true; 
SET io.seqfile.compression.type=BLOCK;
 
-- оптимизированные данные
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
 
-- конверсия сырых в оптимизированные данные
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
 
insert overwrite table kiss partition(`date`)
select
    parsed.p as p, 
    parsed.p2 as p2,
    from_unixtime(cast(parsed.t as int)) as dt, 
    parsed.n as event,
    parsed.campaign_content,
    parsed.campaign_medium,
    parsed.campaign_name,
    parsed.campaign_source,
    parsed.campaign_terms,
    parsed.search_engine,
    parsed.search_terms,
    kiss_raw.json_data, 
    to_date(from_unixtime(cast(parsed.t as int))) as `date`
from kiss_raw 
lateral view json_tuple(regexp_replace(kiss_raw.json_data, '\\\\', 'x'), '_p', '_p2', '_n', '_t', 
    'campaign content',
    'campaign medium',
    'campaign name',
    'campaign source',
    'campaign terms',
    'search engine',
    'search terms'
) parsed as p, p2, n, t,
    campaign_content,
    campaign_medium,
    campaign_name,
    campaign_source,
    campaign_terms,
    search_engine,
    search_terms
;

CREATE EXTERNAL TABLE session_pairs_s3(
  p string, 
  p2 string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://enter-kiss-test/hive/session_pairs'
TBLPROPERTIES (
  'numPartitions'='0', 
  'numFiles'='0', 
  'transient_lastDdlTime'='1376839385', 
  'numRows'='0', 
  'totalSize'='0', 
  'rawDataSize'='0');


create external table sessions (
    session string, 
    alias string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY "," 
location 's3://enter-kiss-test/hive/sessions/';

-- оптимизированные данные
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
 
-- конверсия сырых в оптимизированные данные
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
 
insert overwrite table kiss_normalized partition(`date`)
select
    coalesce(sessions.session, kiss.p), 
    dt, 
    event,
    campaign_content,
    campaign_medium,
    campaign_name,
    campaign_source,
    campaign_terms,
    search_engine,
    search_terms,
    json_data, 
    `date`
from kiss left outer join sessions on kiss.p = sessions.alias 
;



create table user_marker (p string, id string, dt string);

insert into table user_marker select p, order_number, dt from kiss_normalized lateral view json_tuple(kiss_normalized.json_data, 'checkout complete order id') parsed as order_number where event = 'checkout complete';
insert into table user_marker select p, 'start', min(dt) from kiss_normalized group by p;
insert into table user_marker select p, 'end', max(dt) from kiss_normalized group by p;
insert into table user_marker select p, concat('stop-', dt), dt from (
    select p, dt, lag(dt) over (partition by p order by dt) as prev_dt from subkiss
) t
where 
prev_dt is not null and
datediff(to_date(dt), to_date(prev_dt)) >= 14;

create table order_timeslot as 
select 
    p, 
    order_number, 
    lag(dt) over (partition by p order by dt) as start_dt, 
    dt as end_dt 
from user_order;

create table first_last_event as select p, min(dt) as first_dt, max(dt) as last_dt from kiss_normalized group by p;



create table user_sessions as select 
    k.p, 
    o.order_number, 
    k.dt, 
    k.event, 
    k.campaign_content,
    k.campaign_medium,
    k.campaign_name,
    k.campaign_source,
    k.campaign_terms,
    k.search_engine,
    k.search_terms,
    k.json_data
from kiss_normalized k join order_timeslot o on k.p = o.p
where
    k.dt >= coalesce(o.start_dt, '2000-01-01 00:00:00') and
    k.dt <= o.end_dt;


select d, count(*) from
(
    select datediff(prev, cur) d from
    (
        select to_date(dt) cur, to_date(lag(dt)) over (partition by p, order_number order by dt) prev
        from user_sessions
    ) t
) t group by d order by d;
-- компрессия
SET mapred.output.compress=true;
SET hive.exec.compress.output=true; 
SET io.seqfile.compression.type=BLOCK;
 
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;


-- сырые данные
create external table kiss_raw (
    json_data string
)
location 's3://enter-kiss-test/revisions/' ;
 

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


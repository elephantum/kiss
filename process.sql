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


-- TODO формирование таблицы сессий
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


create external table sessions_dict (
    session string, 
    alias string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY "," 
location 's3://enter-kiss-test/hive/sessions/';


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


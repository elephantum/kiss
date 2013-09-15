CREATE EXTERNAL TABLE user_marker(
  p string, 
  id string, 
  dt string)
LOCATION
  's3://enter-kiss-test/enter_proto/user_marker';

CREATE EXTERNAL TABLE kiss_10k(
  p string, 
  dt string, 
  event string, 
  campaign_content string, 
  campaign_medium string, 
  campaign_name string, 
  campaign_source string, 
  campaign_terms string, 
  search_engine string, 
  search_terms string, 
  json_data string, 
  date string)
LOCATION
  's3://enter-kiss-test/enter_proto/kiss_10k/';

create table summary_10k as 
select 
p, 
`date`, 
event, 
count(*) c, 
sum(coalesce(cast(order_total as int), 0)) order_total
from kiss_10k
lateral view json_tuple(kiss_10k.json_data, 'checkout complete order total') ext_data as order_total
group by p, `date`, event
order by p, `date`, event;

create table user_order_counters as select 
p,
`date`, 
order_total,
sum(c) over w as cumulative_order_count,
sum(order_total) OVER w as cumulative_order_total
from summary_10k
where event='checkout complete'
order by p, `date` 
window w as (PARTITION BY p ORDER BY `date` ROWS UNBOUNDED PRECEDING)
;

create table user_start as select
p,
min(`date`) as start
from summary_10k
group by p;



create table order_by_type as select 
oc.p,
oc.`date`,
if(oc.cumulative_order_count <= 1, 'new',
if(datediff(`date`, s.start) / (oc.cumulative_order_count + 1) < 30, 'core',
'xz')) as type
from user_order_counters oc join user_start s on oc.p = s.p
;

select
obt.`date`,
obt.`type`,
sum(s.c),
sum(s.order_total)
from
order_by_type obt join summary_10k s on obt.p = s.p and obt.`date` = s.`date`
where s.event='checkout complete'
group by obt.`date`, obt.`type`
limit 100
;



create table summary_100k as 
select p, `date`, event, count(*) c from kiss_100k group by p, `date`, event
order by p, `date`, event;



CREATE EXTERNAL TABLE kiss_100k(
  p string, 
  dt string, 
  event string, 
  campaign_content string, 
  campaign_medium string, 
  campaign_name string, 
  campaign_source string, 
  campaign_terms string, 
  search_engine string, 
  search_terms string, 
  json_data string, 
  date string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://enter-kiss-test/enter_proto/kiss_100k'
TBLPROPERTIES (
  'transient_lastDdlTime'='1378467295')


CREATE EXTERNAL TABLE ad_campaigns_100k(
  p string, 
  dt string, 
  event string, 
  campaign_content string, 
  campaign_medium string, 
  campaign_name string, 
  campaign_source string, 
  campaign_terms string, 
  search_engine string, 
  search_terms string, 
  json_data string, 
  date string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://enter-kiss-test/enter_proto/ad_campaigns_100k'
TBLPROPERTIES (
  'transient_lastDdlTime'='1379180267')

CREATE EXTERNAL TABLE referrers_100k_s3(
  p string, 
  dt string, 
  event string, 
  campaign_content string, 
  campaign_medium string, 
  campaign_name string, 
  campaign_source string, 
  campaign_terms string, 
  search_engine string, 
  search_terms string, 
  json_data string, 
  date string, 
  referrer string, 
  url string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://enter-kiss-test/enter_proto/referrers_100k'
TBLPROPERTIES (
  'transient_lastDdlTime'='1379182878')

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
LOCATION
  's3://enter-kiss-test/enter_proto/kiss_100k';


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
LOCATION
  's3://enter-kiss-test/enter_proto/ad_campaigns_100k';


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
LOCATION
  's3://enter-kiss-test/enter_proto/referrers_100k';


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



create table olap_daily_summary_10k (
  p string,
  `date` string,
  daily_all_events_count int,
  -- другие поведенческие характеристики
  daily_order_total float,
  daily_order_count int
);


insert into table olap_daily_summary_10k
select
p, 
`date`, 
count(*) daily_all_events_count,
sum(coalesce(cast(ext_data.order_total as float), 0)) daily_order_total,
sum(event == 'checkout complete') daily_order_count
from kiss_10k
lateral view json_tuple(kiss_10k.json_data, 'checkout complete order total') ext_data as order_total
group by p, `date`
order by p, `date`;


create table user_start as select
p,
min(`date`) as start
from summary_10k
group by p;


create table olap_cumulative_summary_10k (
  p string,
  `date` string,

  daily_all_events_count int,
  cumulative_all_events_count int,
  -- другие поведенческие характеристики
  daily_order_total float,
  cumulative_order_total float,
  daily_order_count int,
  cumulative_order_count int,

  client_class string
);


insert into table olap_cumulative_summary_10k
select
  t.p,
  t.`date`,
  t.daily_all_events_count,
  t.cumulative_all_events_count,
  t.daily_order_total,
  t.cumulative_order_total,
  t.daily_order_count,
  t.cumulative_order_count,

  if(t.cumulative_order_count <= 1, 'new',
  if(datediff(t.`date`, user_start.start) / (t.cumulative_order_count + 1) < 30, 'core',
  'xz'))
from (
  select 
  daily.p,
  daily.`date`, 
  daily.daily_all_events_count,
  sum(daily.daily_all_events_count) over w as cumulative_all_events_count,
  daily.daily_order_total,
  sum(daily.daily_order_total) over w as cumulative_order_total,
  daily.daily_order_count,
  sum(daily.daily_order_count) over w as cumulative_order_count
  from olap_daily_summary_10k daily
  window w as (PARTITION BY p ORDER BY `date` ROWS UNBOUNDED PRECEDING)
) t 
join user_start on daily.p = user_start.p
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
limit 100;


create table summary_100k as 
select p, `date`, event, count(*) c from kiss_100k group by p, `date`, event
order by p, `date`, event;




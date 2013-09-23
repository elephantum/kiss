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


create table olap_summary_daily_10k (
  p string,
  days_active_daily int,
  days_with_checkout_daily int,

  ad_campaign_hit_count_daily int,

  cheap_traffic_ad_hit_count_daily int,
  actionpay_ad_hit_count_daily int,
  yandexmarket_ad_hit_count_daily int,
  yandex_ad_hit_count_daily int,
  enter_ad_hit_count_daily int,
  other_ad_hit_count_daily int,

  viewed_product_count_daily int,
  viewed_category_count_daily int,
  add_to_cart_count_daily int,
  view_cart_count_daily int,
  checkout_step_1_count_daily int,
  checkout_complete_count_daily int,

  checkout_complete_sum_daily float
)
partitioned by (`date` string)
location 's3://enter-kiss-test/enter_proto/olap_summary_daily_10k/';
alter table olap_summary_daily_10k recover partitions;


insert overwrite table olap_summary_daily_10k partition(`date`)
select
  p, 
  1,
  if(sum(if(event == 'checkout complete', 1, 0)) > 0, 1, 0),

  sum(if(event == 'ad campaign hit', 1, 0)) ad_campaign_hit_count_daily,

  sum(if(campaign_source_norm == 'cheap_traffic', 1, 0)) cheap_traffic_ad_hit_count_daily,
  sum(if(campaign_source_norm == 'actionpay', 1, 0)) actionpay_ad_hit_count_daily,
  sum(if(campaign_source_norm == 'yandexmarket', 1, 0)) yandexmarket_ad_hit_count_daily,
  sum(if(campaign_source_norm == 'yandex', 1, 0)) yandex_ad_hit_count_daily,
  sum(if(campaign_source_norm == 'enter', 1, 0)) enter_ad_hit_count_daily,
  sum(if(campaign_source_norm == 'other', 1, 0)) other_ad_hit_count_daily,

  sum(if(event == 'viewed product', 1, 0)) viewed_product_count_daily,
  sum(if(event == 'viewed category', 1, 0)) viewed_category_count_daily,
  sum(if(event == 'add to cart', 1, 0)) add_to_cart_count_daily,
  sum(if(event == 'view cart', 1, 0)) view_cart_count_daily,
  sum(if(event == 'checkout step 1', 1, 0)) checkout_step_1_count_daily,
  sum(if(event == 'checkout complete', 1, 0)) checkout_complete_count_daily,

  sum(coalesce(cast(ext_data.order_sum as float), 0)) checkout_complete_sum_daily,

  `date`

from 
  (
    select 
      kiss_10k.*,
      if(campaign_source like 'cheap_traffic', 'cheap_traffic',
      if(campaign_source like 'actionpay', 'actionpay',

      if(campaign_source like 'yandexmarket%', 'yandexmarket',
      if(campaign_source like 'yandex%', 'yandex',
      if(campaign_source like 'enter%', 'enter',

      'other'
      ))))) campaign_source_norm
    from
    kiss_10k
  ) kiss_10k
  lateral view json_tuple(kiss_10k.json_data, 'checkout complete order total') ext_data as order_sum
group by p, `date`
;


create table user_first_event_10k as select
p,
min(`date`) as start
from olap_daily_summary_10k
group by p;


-- и другие поведенческие характеристики
create table olap_cumulative_summary_10k (
  p string,
  `date` string,

  daily_all_events_count int,
  cumulative_all_events_count int,

  daily_order_total float,
  cumulative_order_total float,
  daily_order_count int,
  cumulative_order_count int,

  user_class string
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
  if(datediff(t.`date`, user_first_event_10k.start) / (t.cumulative_order_count + 1) < 30, 'core',
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
join user_first_event_10k on t.p = user_first_event_10k.p
;


create table report_order_total_by_user_class_10k as
select
  `date`,
  user_class,
  sum(daily_order_count),
  sum(daily_order_total)
from olap_cumulative_summary_10k
group by `date`, user_class
order by `date`, user_class;


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




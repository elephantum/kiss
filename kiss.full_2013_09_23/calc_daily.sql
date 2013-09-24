SET mapred.output.compress=true;
SET hive.exec.compress.output=true; 
SET io.seqfile.compression.type=BLOCK;
 
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;


create external table kiss_raw (
    json_data string
)
location 's3://enter-kiss-test/revisions/' ;

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

create table olap_summary_daily_full_2013_09_23 (
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
location 's3://enter-kiss-test/enter_proto_full_2013_09_23/olap_summary_daily_full_2013_09_23/';
alter table olap_summary_daily_full_2013_09_23 recover partitions;


create table olap_summary_cumulative_full_2013_09_23 (
  p string,

  days_active_daily int,
  days_active_lifetime int,
  days_with_checkout_daily int,
  days_with_checkout_lifetime int,

  ad_campaign_hit_count_daily int,
  ad_campaign_hit_count_lifetime int,
  viewed_product_count_daily int,
  viewed_product_count_lifetime int,
  viewed_category_count_daily int,
  viewed_category_count_lifetime int,
  add_to_cart_count_daily int,
  add_to_cart_count_lifetime int,
  view_cart_count_daily int,
  view_cart_count_lifetime int,
  checkout_step_1_count_daily int,
  checkout_step_1_count_lifetime int,
  checkout_complete_count_daily int,
  checkout_complete_count_lifetime int,
  checkout_complete_sum_daily float,
  checkout_complete_sum_lifetime float,

  user_class string
)
partitioned by (`date` string)
location 's3://enter-kiss-test/enter_proto_full_2013_09_23/olap_summary_cumulative_full_2013_09_23/';
alter table olap_summary_cumulative_full_2013_09_23 recover partitions;


insert overwrite table olap_summary_daily_full_2013_09_23 partition(`date`)
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
      kiss.p,
      kiss.`date`,

      kiss.event,
      kiss.json_data,

      if(campaign_source like 'cheap_traffic', 'cheap_traffic',
      if(campaign_source like 'actionpay', 'actionpay',

      if(campaign_source like 'yandexmarket%', 'yandexmarket',
      if(campaign_source like 'yandex%', 'yandex',
      if(campaign_source like 'enter%', 'enter',

      'other'
      ))))) campaign_source_norm
    from
    kiss_full_2013_09_23 kiss
  ) kiss
  lateral view json_tuple(kiss.json_data, 'checkout complete order total') ext_data as order_sum
group by p, `date`
;

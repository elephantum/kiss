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


insert overwrite table kiss_full_2013_09_23 partition(`date`)
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
    regexp_replace(kiss_raw.json_data, '\\\\', 'x'), 
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


create table session_pairs_full_2013_09_23(
  p string, 
  p2 string)
location 's3://enter-kiss-test/enter_proto_full_2013_09_23/session_pairs/';


insert overwrite table session_pairs_full_2013_09_23
select
p, p2
from kiss_full_2013_09_23
where p2 is not null;


create external table session_alias_full_2013_09_23 (
    session string, 
    alias string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY "," 
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
alter table kiss_normalized recover partitions;


insert overwrite table kiss_normalized_full_2013_09_23 partition(`date`)
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
from kiss_full_2013_09_23 kiss
left outer join session_alias_full_2013_09_23 sessions on kiss.p = sessions.alias 
;


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
      kiss_normalized_full_2013_09_23.*,
      if(campaign_source like 'cheap_traffic', 'cheap_traffic',
      if(campaign_source like 'actionpay', 'actionpay',

      if(campaign_source like 'yandexmarket%', 'yandexmarket',
      if(campaign_source like 'yandex%', 'yandex',
      if(campaign_source like 'enter%', 'enter',

      'other'
      ))))) campaign_source_norm
    from
    kiss_normalized_full_2013_09_23
  ) kiss_normalized_full_2013_09_23
  lateral view json_tuple(kiss_full_2013_09_23.json_data, 'checkout complete order total') ext_data as order_sum
group by p, `date`
;

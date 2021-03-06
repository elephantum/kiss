create external table olap_summary_daily_full (
  p string,
  days_active_daily int,

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
location 's3://enter-kiss-test/enter_proto/olap_summary_daily_full/';
alter table olap_summary_daily_full recover partitions;


insert overwrite table olap_summary_daily_full partition(`date`)
select
  p, 
  1,

  sum(if(event == 'ad campaign hit', 1, 0)) ad_campaign_hit_count_daily,
  sum(if(event == 'viewed product', 1, 0)) viewed_product_count_daily,
  sum(if(event == 'viewed category', 1, 0)) viewed_category_count_daily,
  sum(if(event == 'add to cart', 1, 0)) add_to_cart_count_daily,
  sum(if(event == 'view cart', 1, 0)) view_cart_count_daily,
  sum(if(event == 'checkout step 1', 1, 0)) checkout_step_1_count_daily,
  sum(if(event == 'checkout complete', 1, 0)) checkout_complete_count_daily,

  sum(if(campaign_source_norm == 'cheap_traffic', 1, 0)) cheap_traffic_ad_hit_count_daily,
  sum(if(campaign_source_norm == 'actionpay', 1, 0)) actionpay_ad_hit_count_daily,
  sum(if(campaign_source_norm == 'yandexmarket', 1, 0)) yandexmarket_ad_hit_count_daily,
  sum(if(campaign_source_norm == 'yandex', 1, 0)) yandex_ad_hit_count_daily,
  sum(if(campaign_source_norm == 'enter', 1, 0)) enter_ad_hit_count_daily,
  sum(if(campaign_source_norm == 'other', 1, 0)) other_ad_hit_count_daily,

  sum(coalesce(cast(ext_data.order_sum as float), 0)) checkout_complete_sum_daily,

  `date`

from 
  (
    select 
      kiss_normalized.*,
      if(instr(campaign_source, 'cheap_traffic') == 1, 'cheap_traffic',
      if(instr(campaign_source, 'actionpay') == 1, 'actionpay',

      if(instr(campaign_source, 'yandexmarket') == 1, 'yandexmarket',
      if(instr(campaign_source, 'yandex') == 1, 'yandex',
      if(instr(campaign_source, 'enter') == 1, 'enter',

      'other'
      ))))) campaign_source_norm
    from
    kiss_normalized
  ) kiss_normalized
  lateral view json_tuple(kiss_normalized.json_data, 'checkout complete order total') ext_data as order_sum
group by p, `date`
;


create table olap_summary_cumulative_full (
  p string,

  days_active_daily int,
  days_active_lifetime int,

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
location 's3://enter-kiss-test/enter_proto/olap_summary_cumulative_full/';
alter table olap_summary_cumulative_full recover partitions;

insert overwrite table olap_summary_cumulative_full partition(`date`)
select
  t.p,

  days_active_daily,
  days_active_lifetime,

  ad_campaign_hit_count_daily,
  ad_campaign_hit_count_lifetime,
  viewed_product_count_daily,
  viewed_product_count_lifetime,
  viewed_category_count_daily,
  viewed_category_count_lifetime,
  add_to_cart_count_daily,
  add_to_cart_count_lifetime,
  view_cart_count_daily,
  view_cart_count_lifetime,
  checkout_step_1_count_daily,
  checkout_step_1_count_lifetime,
  checkout_complete_count_daily,
  checkout_complete_count_lifetime,
  checkout_complete_sum_daily,
  checkout_complete_sum_lifetime,

  if(t.checkout_complete_count_lifetime = 0, 'lead',
  if(t.checkout_complete_count_lifetime = 1, 'new',
  if(datediff(t.`date`, user_first_event_full.start) / (t.checkout_complete_count_lifetime + 1) < 30, 'core',
  'xz'))) user_class,

  t.`date`

from (
  select 
    p,
    `date`,

    days_active_daily,
    sum(days_active_daily) over w as days_active_lifetime,

    ad_campaign_hit_count_daily,
    sum(ad_campaign_hit_count_daily) over w as ad_campaign_hit_count_lifetime,
    viewed_product_count_daily,
    sum(viewed_product_count_daily) over w as viewed_product_count_lifetime,
    viewed_category_count_daily,
    sum(viewed_category_count_daily) over w as viewed_category_count_lifetime,
    add_to_cart_count_daily,
    sum(add_to_cart_count_daily) over w as add_to_cart_count_lifetime,
    view_cart_count_daily,
    sum(view_cart_count_daily) over w as view_cart_count_lifetime,
    checkout_step_1_count_daily,
    sum(checkout_step_1_count_daily) over w as checkout_step_1_count_lifetime,
    checkout_complete_count_daily,
    sum(checkout_complete_count_daily) over w as checkout_complete_count_lifetime,
    checkout_complete_sum_daily,
    sum(checkout_complete_sum_daily) over w as checkout_complete_sum_lifetime

  from olap_summary_daily_full
  window w as (PARTITION BY p ORDER BY `date` ROWS UNBOUNDED PRECEDING)
) t 
join (
  select
    p,
    min(`date`) as start
  from olap_summary_daily_full
  group by p
) user_first_event_full on t.p = user_first_event_full.p
order by p, user_class
;


create table report_by_user_class_full (
  `date` string,
  user_class string,
  visits int,
  checkout_complete_count_daily int,
  checkout_complete_sum_daily float
)
location 's3://enter-kiss-test/enter_proto/report_order_sum_by_user_class_full/';

insert overwrite table report_by_user_class_full
select
  `date`,
  user_class,
  sum(days_active_daily),
  sum(checkout_complete_count_daily),
  sum(checkout_complete_sum_daily)
from olap_summary_cumulative_full
group by `date`, user_class
order by `date`, user_class;

create table report_by_orders_count_full (
  `date` string,
  order_count_lifetime int,
  visits int,
  checkout_complete_count_daily int,
  checkout_complete_sum_daily float
)
location 's3://enter-kiss-test/enter_proto/report_by_user_class_full/';

insert overwrite table report_by_orders_count_full
select
  `date`,
  checkout_complete_count_lifetime,
  sum(days_active_daily),
  sum(checkout_complete_count_daily),
  sum(checkout_complete_sum_daily)
from olap_summary_cumulative_full
group by `date`, checkout_complete_count_lifetime
order by `date`, checkout_complete_count_lifetime;


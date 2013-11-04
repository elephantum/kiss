create table yandex_sessions as
select p_market.p p, p_market.`date` `date` from olap_summary_daily_normalized_full_2013_10_16 p_market join
(select p, min(`date`) `date` from olap_summary_daily_normalized_full_2013_10_16 p_market group by p) p_firstdate 
on p_market.p = p_firstdate.p and p_market.`date` = p_firstdate.`date`
where p_market.yandexmarket_ad_hit_count_daily > 0;


create table yandexmarket_history (
  `date` string,
  days_from_start int,
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
location 's3://enter-kiss-test/enter_proto_full_2013_10_16/yandexmarket_history/';
 
insert overwrite table yandexmarket_history
select
  summary.`date`,
  datediff(summary.`date`, yandex_sessions.`date`) days_from_start,
  summary.p,

  days_active_daily,
  days_with_checkout_daily,

  ad_campaign_hit_count_daily,

  cheap_traffic_ad_hit_count_daily,
  actionpay_ad_hit_count_daily,
  yandexmarket_ad_hit_count_daily,
  yandex_ad_hit_count_daily,
  enter_ad_hit_count_daily,
  other_ad_hit_count_daily,

  viewed_product_count_daily,
  viewed_category_count_daily,
  add_to_cart_count_daily,
  view_cart_count_daily,
  checkout_step_1_count_daily,
  checkout_complete_count_daily,

  checkout_complete_sum_daily
from olap_summary_daily_normalized_full_2013_10_16 summary 
join yandex_sessions on summary.p = yandex_sessions.p;


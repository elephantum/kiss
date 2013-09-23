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

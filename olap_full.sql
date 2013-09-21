-- и другие поведенческие характеристики
create table olap_summary_daily_full (
  p string,
  `date` string,
  days_active_daily int,

  ad_campaign_hit_count_daily int,
  viewed_product_count_daily int,
  viewed_category_count_daily int,
  add_to_cart_count_daily int,
  view_cart_count_daily int,
  checkout_step_1_count_daily int,
  checkout_complete_count_daily int,
  checkout_complete_sum_daily float
);


insert into table olap_summary_daily_full
select
  p, 
  `date`, 
  1,

  sum(if(event == 'ad campaign hit count daily', 1, 0)) ad_campaign_hit_count_daily,
  sum(if(event == 'viewed product count daily', 1, 0)) viewed_product_count_daily,
  sum(if(event == 'viewed category count daily', 1, 0)) viewed_category_count_daily,
  sum(if(event == 'add to cart count daily', 1, 0)) add_to_cart_count_daily,
  sum(if(event == 'view cart count daily', 1, 0)) view_cart_count_daily,
  sum(if(event == 'checkout step 1 count daily', 1, 0)) checkout_step_1_count_daily,
  sum(if(event == 'checkout complete count daily', 1, 0)) checkout_complete_count_daily,

  sum(coalesce(cast(ext_data.order_sum as float), 0)) checkout_complete_sum_daily

from 
  kiss_normalized
  lateral view json_tuple(kiss_normalized.json_data, 'checkout complete order total') ext_data as order_sum
group by p, `date`
order by p, `date`;


create table user_first_event_full as select
p,
min(`date`) as start
from olap_summary_daily_full
group by p;


-- и другие поведенческие характеристики
create table olap_summary_cumulative_full (
  p string,
  `date` string,

  days_active_daily int,

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
);


insert into table olap_summary_cumulative_full
select
  t.p,
  t.`date`,

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

  if(t.checkout_complete_sum_lifetime = 0, 'lead',
  if(t.checkout_complete_sum_lifetime = 1, 'new',
  if(datediff(t.`date`, user_first_event_full.start) / (t.checkout_complete_sum_lifetime + 1) < 30, 'core',
  'xz')))
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
    sum(checkout_complete_sum_daily) over w as checkout_complete_sum_lifetime,

  from olap_summary_daily_full daily
  window w as (PARTITION BY p ORDER BY `date` ROWS UNBOUNDED PRECEDING)
) t 
join user_first_event_full on t.p = user_first_event_full.p
;


create table report_order_sum_by_user_class_full as
select
  `date`,
  user_class,
  sum(order_count_daily),
  sum(order_sum_daily)
from olap_summary_cumulative_full
group by `date`, user_class
order by `date`, user_class;



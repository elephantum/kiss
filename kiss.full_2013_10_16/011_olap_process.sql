insert overwrite table olap_summary_daily_full_2013_10_16
select
  `date`,

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

  sum(coalesce(cast(kiss.order_sum as float), 0)) checkout_complete_sum_daily

from 
  (
    select 
      kiss.`_p` as p,
      to_date(kiss.dt) as `date`,

      kiss.`_n` as event,
      kiss.checkout_complete_order_total as order_sum,

      if(kiss.campaign_source is null, 'none',
      if(kiss.campaign_source like 'cheap_traffic', 'cheap_traffic',
      if(kiss.campaign_source like 'actionpay', 'actionpay',

      if(kiss.campaign_source like 'yandexmarket%', 'yandexmarket',
      if(kiss.campaign_source like 'yandex%', 'yandex',
      if(kiss.campaign_source like 'enter%', 'enter',

      'other'
      )))))) campaign_source_norm
    from
    kiss_full_2013_10_16 kiss
  ) kiss
group by `date`, p
order by `date`, p;


insert overwrite table olap_summary_daily_normalized_full_2013_10_16
select
  `date`,
  p,

  if(sum(days_active_daily) > 0, 1, 0),
  if(sum(days_with_checkout_daily) > 0, 1, 0),

  sum(ad_campaign_hit_count_daily),

  sum(cheap_traffic_ad_hit_count_daily),
  sum(actionpay_ad_hit_count_daily),
  sum(yandexmarket_ad_hit_count_daily),
  sum(yandex_ad_hit_count_daily),
  sum(enter_ad_hit_count_daily),
  sum(other_ad_hit_count_daily),

  sum(viewed_product_count_daily),
  sum(viewed_category_count_daily),
  sum(add_to_cart_count_daily),
  sum(view_cart_count_daily),
  sum(checkout_step_1_count_daily),
  sum(checkout_complete_count_daily),

  sum(checkout_complete_sum_daily)

from (
  select 
  coalesce(sessions.session, d.p) p,

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

  checkout_complete_sum_daily,

  `date`
  from olap_summary_daily_full_2013_10_16 d
  left outer join session_alias_full_2013_10_16 sessions on d.p = sessions.alias
) d
group by p, `date`;


insert overwrite table olap_summary_cumulative_full_2013_10_16 partition(`date`)
select
  t.p,

  days_active_daily,
  days_active_lifetime,
  days_with_checkout_daily,
  days_with_checkout_lifetime,

  ad_campaign_hit_count_daily,
  ad_campaign_hit_count_lifetime,

  cheap_traffic_ad_hit_count_daily,
  cheap_traffic_ad_hit_count_lifetime,
  actionpay_ad_hit_count_daily,
  actionpay_ad_hit_count_lifetime,
  yandexmarket_ad_hit_count_daily,
  yandexmarket_ad_hit_count_lifetime,
  yandex_ad_hit_count_daily,
  yandex_ad_hit_count_lifetime,
  enter_ad_hit_count_daily,
  enter_ad_hit_count_lifetime,
  other_ad_hit_count_daily,
  other_ad_hit_count_lifetime,

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
    days_with_checkout_daily,
    sum(days_with_checkout_daily) over w as days_with_checkout_lifetime,

    ad_campaign_hit_count_daily,
    sum(ad_campaign_hit_count_daily) over w as ad_campaign_hit_count_lifetime,

    cheap_traffic_ad_hit_count_daily,
    sum(cheap_traffic_ad_hit_count_daily) over w as cheap_traffic_ad_hit_count_lifetime,
    actionpay_ad_hit_count_daily,
    sum(actionpay_ad_hit_count_daily) over w as actionpay_ad_hit_count_lifetime,
    yandexmarket_ad_hit_count_daily,
    sum(yandexmarket_ad_hit_count_daily) over w as yandexmarket_ad_hit_count_lifetime,
    yandex_ad_hit_count_daily,
    sum(yandex_ad_hit_count_daily) over w as yandex_ad_hit_count_lifetime,
    enter_ad_hit_count_daily,
    sum(enter_ad_hit_count_daily) over w as enter_ad_hit_count_lifetime,
    other_ad_hit_count_daily,
    sum(other_ad_hit_count_daily) over w as other_ad_hit_count_lifetime,

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

  from olap_summary_daily_normalized_full_2013_10_16
  window w as (PARTITION BY p ORDER BY `date` ROWS UNBOUNDED PRECEDING)
) t 
join (
  select
    p,
    min(`date`) as start
  from olap_summary_daily_normalized_full_2013_10_16
  group by p
) user_first_event_full on t.p = user_first_event_full.p
order by p, user_class
;

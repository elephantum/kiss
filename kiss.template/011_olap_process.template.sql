insert overwrite table olap_summary_daily_{data_source} partition(`date`)
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
      kiss_normalized_{data_source}.*,
      if(campaign_source like 'cheap_traffic', 'cheap_traffic',
      if(campaign_source like 'actionpay', 'actionpay',

      if(campaign_source like 'yandexmarket%', 'yandexmarket',
      if(campaign_source like 'yandex%', 'yandex',
      if(campaign_source like 'enter%', 'enter',

      'other'
      ))))) campaign_source_norm
    from
    kiss_normalized_{data_source}
  ) kiss_normalized_{data_source}
  lateral view json_tuple(kiss_{data_source}.json_data, 'checkout complete order total') ext_data as order_sum
group by p, `date`
;

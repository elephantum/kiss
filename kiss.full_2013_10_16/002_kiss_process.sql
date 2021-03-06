-- In [22]: r = re.compile(r'([^\\])\\(\d)')
-- In [23]: r.sub(r'\1x\2', i)


insert overwrite table kiss_full_2013_10_16
select
    from_unixtime(cast(parsed.`_t` as int)) as dt, 
    parsed.`_n`,
    parsed.`_p`,
    parsed.`_p2`,
    parsed.`_t`,
    parsed.`add_to_cart_category_id`,
    parsed.`add_to_cart_category_name`,
    parsed.`add_to_cart_f1_quantity`,
    parsed.`add_to_cart_page_url`,
    parsed.`add_to_cart_product_name`,
    parsed.`add_to_cart_root_category`,
    parsed.`add_to_cart_root_id`,
    parsed.`add_to_cart_sku`,
    parsed.`add_to_cart_sku_price`,
    parsed.`add_to_cart_sku_quantity`,
    parsed.`banner_id`,
    parsed.`banner_url`,
    parsed.`campaign_content`,
    parsed.`campaign_medium`,
    parsed.`campaign_name`,
    parsed.`campaign_source`,
    parsed.`campaign_terms`,
    parsed.`category_results_clicked_category_id`,
    parsed.`category_results_clicked_category_level`,
    parsed.`category_results_clicked_category_name`,
    parsed.`category_results_clicked_category_type`,
    parsed.`category_results_clicked_page_number`,
    parsed.`category_results_clicked_parent_category`,
    parsed.`category_results_clicked_product_name`,
    parsed.`category_results_clicked_product_position`,
    parsed.`category_results_clicked_sku`,
    parsed.`center_image_url`,
    parsed.`checkout_complete_category_name`,
    parsed.`checkout_complete_delivery`,
    parsed.`checkout_complete_delivery_total`,
    parsed.`checkout_complete_order_id`,
    parsed.`checkout_complete_order_total`,
    parsed.`checkout_complete_order_type`,
    parsed.`checkout_complete_parent_category`,
    parsed.`checkout_complete_payment`,
    parsed.`checkout_complete_sku`,
    parsed.`checkout_complete_sku_price`,
    parsed.`checkout_complete_sku_quantity`,
    parsed.`checkout_complete_sku_total`,
    parsed.`checkout_step_1_order_total`,
    parsed.`checkout_step_1_order_type`,
    parsed.`checkout_step_1_sku_quantity`,
    parsed.`checkout_step_1_sku_total`,
    parsed.`informer`,
    parsed.`optimizely_filters`,
    parsed.`referrer`,
    parsed.`returning`,
    parsed.`search_engine`,
    parsed.`search_items_found`,
    parsed.`search_page_url`,
    parsed.`search_string`,
    parsed.`search_terms`,
    parsed.`url`,
    parsed.`view_cart_sku_quantity`,
    parsed.`view_cart_sku_total`,
    parsed.`viewed_category_category_id`,
    parsed.`viewed_category_category_level`,
    parsed.`viewed_category_category_name`,
    parsed.`viewed_category_category_type`,
    parsed.`viewed_category_parent_category`,
    parsed.`viewed_product_product_name`,
    parsed.`viewed_product_product_status`,
    parsed.`viewed_product_sku`
from kiss_raw 
lateral view json_tuple(regexp_replace(kiss_raw.json_data, '\\\\', 'x'), 
    "_n",
    "_p",
    "_p2",
    "_t",
    "add to cart category id",
    "add to cart category name",
    "add to cart f1 quantity",
    "add to cart page url",
    "add to cart product name",
    "add to cart root category",
    "add to cart root id",
    "add to cart sku",
    "add to cart sku price",
    "add to cart sku quantity",
    "banner id",
    "banner url",
    "campaign content",
    "campaign medium",
    "campaign name",
    "campaign source",
    "campaign terms",
    "category results clicked category id",
    "category results clicked category level",
    "category results clicked category name",
    "category results clicked category type",
    "category results clicked page number",
    "category results clicked parent category",
    "category results clicked product name",
    "category results clicked product position",
    "category results clicked sku",
    "center image url",
    "checkout complete category name",
    "checkout complete delivery",
    "checkout complete delivery total",
    "checkout complete order id",
    "checkout complete order total",
    "checkout complete order type",
    "checkout complete parent category",
    "checkout complete payment",
    "checkout complete sku",
    "checkout complete sku price",
    "checkout complete sku quantity",
    "checkout complete sku total",
    "checkout step 1 order total",
    "checkout step 1 order type",
    "checkout step 1 sku quantity",
    "checkout step 1 sku total",
    "informer",
    "optimizely_filters",
    "referrer",
    "returning",
    "search engine",
    "search items found",
    "search page url",
    "search string",
    "search terms",
    "url",
    "view cart sku quantity",
    "view cart sku total",
    "viewed category category id",
    "viewed category category level",
    "viewed category category name",
    "viewed category category type",
    "viewed category parent category",
    "viewed product product name",
    "viewed product product status",
    "viewed product sku")
parsed as
    `_n`,
    `_p`,
    `_p2`,
    `_t`,
    `add_to_cart_category_id`,
    `add_to_cart_category_name`,
    `add_to_cart_f1_quantity`,
    `add_to_cart_page_url`,
    `add_to_cart_product_name`,
    `add_to_cart_root_category`,
    `add_to_cart_root_id`,
    `add_to_cart_sku`,
    `add_to_cart_sku_price`,
    `add_to_cart_sku_quantity`,
    `banner_id`,
    `banner_url`,
    `campaign_content`,
    `campaign_medium`,
    `campaign_name`,
    `campaign_source`,
    `campaign_terms`,
    `category_results_clicked_category_id`,
    `category_results_clicked_category_level`,
    `category_results_clicked_category_name`,
    `category_results_clicked_category_type`,
    `category_results_clicked_page_number`,
    `category_results_clicked_parent_category`,
    `category_results_clicked_product_name`,
    `category_results_clicked_product_position`,
    `category_results_clicked_sku`,
    `center_image_url`,
    `checkout_complete_category_name`,
    `checkout_complete_delivery`,
    `checkout_complete_delivery_total`,
    `checkout_complete_order_id`,
    `checkout_complete_order_total`,
    `checkout_complete_order_type`,
    `checkout_complete_parent_category`,
    `checkout_complete_payment`,
    `checkout_complete_sku`,
    `checkout_complete_sku_price`,
    `checkout_complete_sku_quantity`,
    `checkout_complete_sku_total`,
    `checkout_step_1_order_total`,
    `checkout_step_1_order_type`,
    `checkout_step_1_sku_quantity`,
    `checkout_step_1_sku_total`,
    `informer`,
    `optimizely_filters`,
    `referrer`,
    `returning`,
    `search_engine`,
    `search_items_found`,
    `search_page_url`,
    `search_string`,
    `search_terms`,
    `url`,
    `view_cart_sku_quantity`,
    `view_cart_sku_total`,
    `viewed_category_category_id`,
    `viewed_category_category_level`,
    `viewed_category_category_name`,
    `viewed_category_category_type`,
    `viewed_category_parent_category`,
    `viewed_product_product_name`,
    `viewed_product_product_status`,
    `viewed_product_sku`;


insert overwrite table session_pairs_full_2013_10_16
select
`_p`, `_p2`
from kiss_full_2013_10_16
where `_p2` is not null;


-- !!!! clusters.r
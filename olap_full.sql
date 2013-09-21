-- и другие поведенческие характеристики
create table olap_summary_full_daily (
  p string,
  `date` string,
  all_events_count_daily int,
  order_total_daily float,
  order_count_daily int
);


insert into table olap_summary_full_daily
select
p, 
`date`, 
count(*) all_events_count_daily,
sum(coalesce(cast(ext_data.order_total as float), 0)) order_total_daily,
sum(if(event == 'checkout complete', 1, 0)) order_count_daily
from kiss_full
lateral view json_tuple(kiss_full.json_data, 'checkout complete order total') ext_data as order_total
group by p, `date`
order by p, `date`;


create table user_first_event_full as select
p,
min(`date`) as start
from olap_summary_full_daily
group by p;


-- и другие поведенческие характеристики
create table olap_summary_full_cumulative (
  p string,
  `date` string,

  all_events_count_daily int,
  all_events_count_cumulative int,

  order_total_daily float,
  order_total_cumulative float,
  order_count_daily int,
  order_count_cumulative int,

  user_class string
);


insert into table olap_summary_full_cumulative
select
  t.p,
  t.`date`,
  t.all_events_count_daily,
  t.all_events_count_cumulative,
  t.order_total_daily,
  t.order_total_cumulative,
  t.order_count_daily,
  t.order_count_cumulative,

  if(t.order_count_cumulative = 0, 'lead',
  if(t.order_count_cumulative = 1, 'new',
  if(datediff(t.`date`, user_first_event_full.start) / (t.order_count_cumulative + 1) < 30, 'core',
  'xz')))
from (
  select 
  daily.p,
  daily.`date`, 
  daily.all_events_count_daily,
  sum(daily.all_events_count_daily) over w as all_events_count_cumulative,
  daily.order_total_daily,
  sum(daily.order_total_daily) over w as order_total_cumulative,
  daily.order_count_daily,
  sum(daily.order_count_daily) over w as order_count_cumulative
  from olap_summary_full_daily daily
  window w as (PARTITION BY p ORDER BY `date` ROWS UNBOUNDED PRECEDING)
) t 
join user_first_event_full on t.p = user_first_event_full.p
;


create table report_order_total_by_user_class_full as
select
  `date`,
  user_class,
  sum(order_count_daily),
  sum(order_total_daily)
from olap_summary_full_cumulative
group by `date`, user_class
order by `date`, user_class;



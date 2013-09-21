-- и другие поведенческие характеристики
create table olap_daily_summary_100k (
  p string,
  `date` string,
  daily_all_events_count int,
  daily_order_total float,
  daily_order_count int
);


insert into table olap_daily_summary_100k
select
p, 
`date`, 
count(*) daily_all_events_count,
sum(coalesce(cast(ext_data.order_total as float), 0)) daily_order_total,
sum(if(event == 'checkout complete', 1, 0)) daily_order_count
from kiss_100k
lateral view json_tuple(kiss_100k.json_data, 'checkout complete order total') ext_data as order_total
group by p, `date`
order by p, `date`;


create table user_first_event_100k as select
p,
min(`date`) as start
from olap_daily_summary_100k
group by p;


-- и другие поведенческие характеристики
create table olap_cumulative_summary_100k (
  p string,
  `date` string,

  daily_all_events_count int,
  cumulative_all_events_count int,

  daily_order_total float,
  cumulative_order_total float,
  daily_order_count int,
  cumulative_order_count int,

  user_class string
);


insert into table olap_cumulative_summary_100k
select
  t.p,
  t.`date`,
  t.daily_all_events_count,
  t.cumulative_all_events_count,
  t.daily_order_total,
  t.cumulative_order_total,
  t.daily_order_count,
  t.cumulative_order_count,

  if(t.cumulative_order_count = 0, 'lead',
  if(t.cumulative_order_count = 1, 'new',
  if(datediff(t.`date`, user_first_event_100k.start) / (t.cumulative_order_count + 1) < 30, 'core',
  'xz')))
from (
  select 
  daily.p,
  daily.`date`, 
  daily.daily_all_events_count,
  sum(daily.daily_all_events_count) over w as cumulative_all_events_count,
  daily.daily_order_total,
  sum(daily.daily_order_total) over w as cumulative_order_total,
  daily.daily_order_count,
  sum(daily.daily_order_count) over w as cumulative_order_count
  from olap_daily_summary_100k daily
  window w as (PARTITION BY p ORDER BY `date` ROWS UNBOUNDED PRECEDING)
) t 
join user_first_event_100k on t.p = user_first_event_100k.p
;


create table report_order_total_by_user_class_100k as
select
  `date`,
  user_class,
  sum(daily_order_count),
  sum(daily_order_total)
from olap_cumulative_summary_100k
group by `date`, user_class
order by `date`, user_class;



create table user_marker (p string, id string, dt string);

insert into table user_marker select p, order_number, dt from kiss_normalized lateral view json_tuple(kiss_normalized.json_data, 'checkout complete order id') parsed as order_number where event = 'checkout complete';
insert into table user_marker select p, 'start', min(dt) from kiss_normalized group by p;
insert into table user_marker select p, 'end', max(dt) from kiss_normalized group by p;
insert into table user_marker select p, concat('stop-', dt), dt from (
    select p, dt, lag(dt) over (partition by p order by dt) as prev_dt from subkiss
) t
where 
prev_dt is not null and
datediff(to_date(dt), to_date(prev_dt)) >= 14;

create table order_timeslot as 
select 
    p, 
    order_number, 
    lag(dt) over (partition by p order by dt) as start_dt, 
    dt as end_dt 
from user_order;

create table first_last_event as select p, min(dt) as first_dt, max(dt) as last_dt from kiss_normalized group by p;



select d, count(*) from
(
    select datediff(prev, cur) d from
    (
        select to_date(dt) cur, to_date(lag(dt)) over (partition by p, order_number order by dt) prev
        from user_sessions
    ) t
) t group by d order by d;
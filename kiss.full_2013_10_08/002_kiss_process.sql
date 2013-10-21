insert overwrite table kiss_full_2013_10_08
select
    parsed.p as p, 
    parsed.p2 as p2,
    from_unixtime(cast(parsed.t as int)) as dt, 
    parsed.n as event,
    regexp_replace(kiss_raw.json_data, '\\\\', 'x')
from kiss_raw 
lateral view json_tuple(regexp_replace(kiss_raw.json_data, '\\\\', 'x'), '_p', '_p2', '_n', '_t') parsed as p, p2, n, t;


insert overwrite table session_pairs_full_2013_10_08
select
p, p2
from kiss_full_2013_10_08
where p2 is not null;


-- !!!! clusters.r
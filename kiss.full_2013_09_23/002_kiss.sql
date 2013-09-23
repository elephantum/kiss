insert overwrite table kiss_full_2013_09_23 partition(`date`)
select
    parsed.p as p, 
    parsed.p2 as p2,
    from_unixtime(cast(parsed.t as int)) as dt, 
    parsed.n as event,
    parsed.campaign_content,
    parsed.campaign_medium,
    parsed.campaign_name,
    parsed.campaign_source,
    parsed.campaign_terms,
    parsed.search_engine,
    parsed.search_terms,
    regexp_replace(kiss_raw.json_data, '\\\\', 'x'), 
    to_date(from_unixtime(cast(parsed.t as int))) as `date`
from kiss_raw 
lateral view json_tuple(regexp_replace(kiss_raw.json_data, '\\\\', 'x'), '_p', '_p2', '_n', '_t', 
    'campaign content',
    'campaign medium',
    'campaign name',
    'campaign source',
    'campaign terms',
    'search engine',
    'search terms'
) parsed as p, p2, n, t,
    campaign_content,
    campaign_medium,
    campaign_name,
    campaign_source,
    campaign_terms,
    search_engine,
    search_terms
;


insert overwrite table session_pairs_full_2013_09_23
select
p, p2
from kiss_full_2013_09_23
where p2 is not null;


-- !!!! clusters.r
-- конверсия сырых в оптимизированные данные
insert overwrite table kiss partition(`date`)
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
    kiss_raw.json_data, 
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


-- конверсия сырых в оптимизированные данные
insert overwrite table kiss_normalized partition(`date`)
select
    coalesce(sessions.session, kiss.p), 
    dt, 
    event,
    campaign_content,
    campaign_medium,
    campaign_name,
    campaign_source,
    campaign_terms,
    search_engine,
    search_terms,
    json_data, 
    `date`
from kiss left outer join sessions on kiss.p = sessions.alias 
;


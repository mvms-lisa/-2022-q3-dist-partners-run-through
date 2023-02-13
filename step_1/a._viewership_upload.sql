-- Amagi Q4 Upload
copy into AMAGI_VIEWERSHIP(
view_date,
platform_channel_name,
platform_content_id,
platform_content_name,
series,
sessions,	
tot_mov,
unique_viewers,
avg_session_count,
avg_duration_per_session,
avg_duration_per_viewer,
month,
year_month_day,
ref_id,
content_provider,
share,
revenue,
year, 
quarter,
filename) 
from (
    select t.$1, t.$2, t.$3, t.$4, t.$5,
    to_number(REPLACE(t.$6, ','),12, 2),
    to_number(REPLACE(t.$7, ','), 12, 2),
    to_number(REPLACE(t.$8, ','), 12, 2),
    to_number(REPLACE(t.$9, ','), 8, 2),
    to_number(REPLACE(t.$10, ','), 8, 2),
    to_number(REPLACE(t.$11, ','), 8, 2), t.$12, t.$13, t.$14, t.$15,
    to_number(REPLACE(t.$17, '%'), 11, 8),
    to_number(REPLACE(t.$18, '$'), 15, 5),
    2022, 'q3', 'amagi_register_q3_2022.csv'  from @distribution_partners t) pattern='.*amagi_register_q3_2022.*' file_format = nosey_viewership 
ON_ERROR=SKIP_FILE;

-- AMAGI UPDATES
call territory_update_amagi_viewership('q3', 2022);

-- deal_parent update
call deal_parent_update_amagi('q3', 2022);

-- update tot_hov 
update amagi_viewership 
set tot_hov = tot_mov / 60 
where tot_hov is null and year = 2022 and quarter = 'q3'   

--  series id update
update amagi_viewership a 
set a.series_id = q.series_id
from (
  select a.id as id, a.platform_content_id, a.formatted_title, a.ref_id, a.series, s.term, a.content_provider, s.series_id as series_id
  from amagi_viewership a 
  join dictionary.public.series s on (s.entry = a.series)
  where year = 2022 and quarter = 'q3' 
)q
where q.id = a.id


-- partner update
update amagi_viewership a 
set a.partner = q.partner, a.viewership_type = q.dtype
from (
  select a.id as id,  a.series_id, d.id as series_id, d.partner as partner, a.partner as apartner, d.type as dtype
  from amagi_viewership a 
  join dictionary.public.deals d on (d.id = a.deal_parent)
  where year = 2022 and quarter = 'q3' 
)q
where q.id = a.id

-- tag as updated
update amagi_viewership
set inital_update = 'true'
where year = 2022 and quarter = 'q3' and inital_update is null 


-- PLUTO

-- Pluto US load from REGISTER
 copy into PLUTO_VIEWERSHIP(year_month_day, series, platform_content_name, tot_mov, sessions, revenue, ref_id, content_provider, share, deal_parent, territory_id, year, quarter,filename)
        from (select t.$1, t.$2, t.$3, to_number(REPLACE(t.$4, ','), 25, 5), to_number(REPLACE(t.$5, ','), 38, 0), to_decimal(REPLACE(REPLACE(t.$6, '$'), ','), 10, 3), t.$7, t.$8, to_number(REPLACE(t.$10, '%'), 10,7),
        29, 1, 2022, 'q3', 'only_US_pluto_register_q3_22.csv'
        from @distribution_partners t) pattern='.*only_US_pluto_register_q3_22.*' file_format = nosey_viewership 
        ON_ERROR=SKIP_FILE FORCE=TRUE;
        
-- Pluto non-US
copy into PLUTO_VIEWERSHIP(platform_content_name, revenue, month, year_month_day, ref_id, content_provider, series, partner, territory, territory_id, year, quarter, filename) 
from (select t.$1, to_number(REPLACE(REPLACE(REPLACE(t.$2, '-', 0), '$'), ','), 12,2), t.$3, t.$4, t.$5,t.$6,t.$7,t.$8, t.$9, t.$10, 
      2022, 'q3', 'non_US_pluto_register_q3_22.csv' from @distribution_partners t) pattern='.*non_US_pluto_register_q3_22.*' file_format = nosey_viewership 
ON_ERROR=SKIP_FILE FORCE=TRUE;
        
-- set title = platform_content_name where platform_content_name has a ":", use the get_title_pluto function to derive title
update pluto_viewership 
set title = get_title_pluto(platform_content_name)
where contains(platform_content_name, ':') and year = 2022 and quarter = 'q3' 


-- CLEAN UP TITLES MAKE FUNCTION
update pluto_viewership
set formatted_title = TRIM(REGEXP_REPLACE(  title, '\\([^()]*\\)'  , ''))
where (ref_id = 'externalId' or ref_id is null) and  quarter = 'q3' and year = 2022  and length(title)> 0

    -- If all clip names are in the right format update using
    update pluto_viewership 
    set title = get_long_title_pluto(platform_content_name)
    where contains(platform_content_name, ':') and year = 2022 and quarter = 'q3' and title is null
    
    
--  series id update
update pluto_viewership p
set p.series_id = q.series_id
from (
  select p.id as id, p.ref_id, p.series, s.term, p.content_provider, s.series_id as series_id
  from pluto_viewership p
  join dictionary.public.series s on (s.entry = p.series)
  where year = 2022 and quarter = 'q3'
)q
where q.id = p.id



-- Periscope
    -- Update quarter, filename, pattern
copy into periscope_viewership(channel, platform_content_name, play_count, partner_formatted_time, asset_duration, avg_play_time, avg_playthrough_rate, asset_id, year_month_day, share, ref_id, content_provider, revenue, deal_parent, year, quarter, filename)
from (select t.$1, t.$2, to_number(t.$3, 7, 0), t.$4, t.$5, t.$6, to_number(t.$7, 11, 10), t.$8, t.$9, to_decimal(REPLACE(t.$10, '%'), 11, 8), t.$11, t.$12, to_number(REPLACE(t.$13, '$'), 15, 6), 17, 2022, 'q3', 'xumo_periscope_register_q3_2022.csv'
from @distribution_partners t) pattern='.*xumo_periscope_register_q3_2022.*' file_format = nosey_viewership
ON_ERROR=SKIP_FILE FORCE=TRUE


-- series update periscope
update periscope_viewership g
set g.series = q.series, g.series_id = q.series_id
from (
  select g.id as id, a.series as series, a.series_id from periscope_viewership g
  join dictionary.public.assets a on (a.ref_id = g.ref_id)
  where g.year = 2022 and g.quarter = 'q3' and g.series is null
)q
where g.id = q.id

select * from periscope_viewership


-- FREEVEE
-- freevee linear us
copy into freevee_viewership (
    platform_content_name, tot_hov, channel, month, year_month_day, ref_id, content_provider, series, share, revenue,
    quarter, year, deal_parent, platform, territory, territory_id, filename)
from (
    select
        t.$1, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10, t.$11, to_decimal(REPLACE(t.$12, '%'), 11, 8), to_number(REPLACE(REPLACE(t.$13, '$'), ','), 20,5),
        'q3', 2022, 37, 'FreeVee', 'United States', 1, 'freevee_US_register_q3_2022.csv'
    from @distribution_partners t) pattern='.*freevee_US_register_q3_2022.*' file_format = nosey_viewership 
    ON_ERROR=SKIP_FILE FORCE=TRUE;

update freevee_viewership
set partner = 'FreeVee Linear'
where year = 2022 and quarter = 'q3' and filename = 'freevee_US_register_q3_2022.csv'
    
-- freevee uk
copy into freevee_viewership (
    view_date, platform_content_name, series, tot_mov_uk, unique_streams_uk, month, year_month_day, ref_id, content_provider, share, revenue,
    quarter, year, deal_parent, platform, territory, territory_id, filename) 
from (
    select 
           t.$1, t.$2, t.$3, to_number(REPLACE(t.$4, ','), 20, 5), t.$5, t.$6, t.$7, t.$8, t.$9, to_decimal(REPLACE(t.$11, '%'), 11, 8), to_number(REPLACE(t.$12, '$'), 20, 5),
           'q3', 2022, 38, 'FreeVee', 'United Kingdom', 5, 'freevee_vod_register_q3_2022.csv'
    from @distribution_partners t) pattern='.*freevee_vod_register_q3_2022.*' file_format = nosey_viewership 
    ON_ERROR=SKIP_FILE FORCE=TRUE;
            
--freevee
select * from freevee_viewership where year = 2022 and quarter = 'q3' and filename = 'freevee_US_register_q3_2022.csv'

--WURL

-- Select
select partner, deal_parent, territory from wurl_viewership where year = 2022 and quarter = 'q2'
group by partner, deal_parent, territory

select partner, deal_parent, territory from amagi_viewership where year = 2022 and quarter = 'q2'
group by partner, deal_parent, territory

-- Select
select partner, deal_parent, territory from wurl_viewership where year = 2022 and quarter = 'q3'
group by partner, deal_parent, territory

select partner, deal_parent, territory from amagi_viewership where year = 2022 and quarter = 'q3'
group by partner, deal_parent, territory


--plex register
copy into wurl_viewership (date, channel, title, series, ref_id, tot_hov, month, year_month_day, content_provider, share, revenue, year, quarter, filename)
from (select 
            t.$1, 
            t.$2,
            t.$3, 
            t.$4, 
            t.$5, 
            to_number(REPLACE(t.$6, ','), 5, 2), 
            t.$7, 
            t.$8,  
            t.$9,
            to_decimal(REPLACE(t.$10, '%'), 11, 8),
            to_number(REPLACE(t.$11, '$'), 15, 6),   
            2022, 'q3', 'plex_register_q3_2022.csv'
        from @distribution_partners t) pattern='.*plex_register_q3_2022.*' file_format = nosey_viewership 
        ON_ERROR=SKIP_FILE FORCE=TRUE;
        
        
--  series id update
update wurl_viewership a 
set a.series_id = q.series_id
from (
  select a.id as id, a.platform_content_id, a.formatted_title, a.ref_id, a.series, s.term, a.content_provider, s.series_id as series_id
  from wurl_viewership a 
  join dictionary.public.series s on (s.entry = a.series)
  where year = 2022 and quarter = 'q3' and a.series_id is null
)q
where q.id = a.id


--samsung mx, br, us register
copy into wurl_viewership (date, channel, title, asset_duration, tot_hov, tot_sessions, impressions, month, year_month_day, ref_id, content_provider, series, share, revenue, year, quarter, filename)
from (select 
            t.$1, 
            t.$2,
            t.$4, 
            t.$5, 
            to_number(REPLACE(t.$6, ','), 20, 5), 
            t.$7,   
            t.$9,
            t.$10,
            t.$11,
            t.$12,
            t.$13,
            t.$14,
            to_decimal(REPLACE(t.$15, '%'), 11, 8),
            to_number(REPLACE(t.$16, '$'), 15, 6),   
            2022, 'q3', 'samsung_mx_register_q3_2022.csv'
        from @distribution_partners t) pattern='.*samsung_mx_register_q3_2022.*' file_format = nosey_viewership 
        ON_ERROR=SKIP_FILE FORCE=TRUE;


    -- Youtube (from register)
    copy into youtube_viewership(month, year_month_day, title, publish_date, tot_hov, estimated_revenue, ref_id, content_provider, series, share, deal_parent, year, quarter, filename)
    from (select t.$1, t.$2, t.$3, t.$4, to_number(REPLACE(t.$5, ','), 20, 5), to_number(REPLACE(REPLACE(REPLACE(t.$6, '$'),','),'-'), 20, 4), t.$7, t.$8, t.$9, to_number(REPLACE(t.$10, '%'), 11, 8), 42, 2022, 'q3', 'youtube_register_q3_2022.csv'
    from @distribution_partners t) pattern='.*youtube_register_q3_2022.*' file_format = nosey_viewership 
    ON_ERROR=SKIP_FILE FORCE=TRUE;
    

    
    -- Tubi Linear
    copy into giant_interactive_viewership(platform_content_id, platform_content_name, tot_sessions, tot_mov, month, year_month_day, ref_id, content_provider, series, share, revenue, deal_parent, channel_id, year, quarter, filename)
    from (select t.$1, t.$2, to_number(REPLACE(t.$3, ','), 9, 2), to_number(REPLACE(t.$4, ','), 30, 8), t.$5, t.$6, t.$7, t.$8, t.$9, to_number(REPLACE(t.$10, '%'), 11, 8), to_number(REPLACE(t.$11, '$'), 15, 6), 39, 8, 2022, 'q3', 'tubi_linear_register_q3_2022.csv'
    from @distribution_partners t) pattern='.*tubi_linear_register_q3_2022.*' file_format = nosey_viewership 
    ON_ERROR=SKIP_FILE FORCE=TRUE;
    
--  series id update
update giant_interactive_viewership a 
set a.series_id = q.series_id
from (
  select a.id as id, a.platform_content_id, a.formatted_title, a.ref_id, a.series, s.term, a.content_provider, s.series_id as series_id
  from giant_interactive_viewership a 
  join dictionary.public.series s on (s.entry = a.series)
  where year = 2022 and quarter = 'q3' and a.series_id is null
)q
where q.id = a.id
    
-- VIZIO
copy into wurl_viewership (date, channel, partner, title, tot_hov, tot_sessions, mov_per_session, tot_completions, month, year_month_day, ref_id, content_provider, series, share, revenue, year, quarter, filename)
from (select 
            t.$1, 
            t.$2,
            t.$3,
            t.$4, 
            t.$5, 
            t.$6,
            t.$7, 
            t.$8,
            t.$9,
            t.$10,
            t.$11,
            t.$12,
            t.$13,
            to_decimal(REPLACE(t.$14, '%'), 11, 8),
            to_number(REPLACE(t.$15, '$'), 15, 6),   
            2022, 'q3', 'vizio_register_q3_2022.csv'
        from @distribution_partners t) pattern='.*vizio_register_q3_2022.*' file_format = nosey_viewership 
        ON_ERROR=SKIP_FILE FORCE=TRUE;
        
-- trc linear
copy into wurl_viewership (title, series_num, episode_number, tot_hov, channel, month, year_month_day, ref_id, content_provider, series, share, revenue, year, quarter, filename)
from (select 
            t.$1, 
            t.$2,
            t.$3,
            to_number(REPLACE(t.$4, ','), 20, 5), 
            t.$5, 
            t.$6,
            t.$7, 
            t.$8,
            t.$9,
            t.$10,
            to_decimal(REPLACE(t.$11, '%'), 11, 8),
            to_number(REPLACE(REPLACE(t.$12, '$'), ','), 15, 6),   
            2022, 'q3', 'trc_linear_register_q3_2022.csv'
        from @distribution_partners t) pattern='.*trc_linear_register_q3_2022.*' file_format = nosey_viewership 
        ON_ERROR=SKIP_FILE FORCE=TRUE;
        
-- xumo linear        
copy into wurl_viewership (date, channel, partner, title, asset_duration, tot_hov, tot_sessions, impressions, month, year_month_day, ref_id, content_provider, series, share, monthly_share, revenue, deal_parent, year, quarter, filename)
from (select 
            t.$1, 
            t.$2,
            t.$3,
            t.$4,
            t.$5, 
            t.$6,
            t.$7, 
            t.$8,
            t.$9,
            t.$10,
            t.$11,
            t.$12,
            t.$13, 
            to_decimal(REPLACE(t.$14, '%'), 11, 8),
            to_decimal(REPLACE(t.$15, '%'), 6, 3),
            to_number(REPLACE(REPLACE(t.$16, '$'), ','), 15, 6), 
            44, 2022, 'q3', 'xumo_linear_register_q3_2022.csv'
        from @distribution_partners t) pattern='.*xumo_linear_register_q3_2022.*' file_format = nosey_viewership 
        ON_ERROR=SKIP_FILE FORCE=TRUE;
        
        
        -- TRC VOD (data usually comes from the '20xx 1x TRC Nosey Baxter LLC Payout Report but might now include revenue from Roku dashboard)
        copy into wurl_viewership(platform_content_name, revenue, ref_id, series, content_provider, share, deal_parent, year, quarter, filename) 
        from (select  
                t.$1, 
                to_number(REPLACE(REPLACE(t.$2, ','), '$'), 15, 6),
                t.$3, t.$4, t.$5,
                to_number(REPLACE(t.$6, '%'), 11, 8),
                25, 2022, 'q3','trc_vod_register_q3_2022.csv'
        from @distribution_partners t) pattern='.*trc_vod_register_q3_2022.*' file_format = nosey_viewership 
        ON_ERROR=SKIP_FILE FORCE=TRUE;
        

-- stirr xiaomi        
copy into wurl_viewership (date, channel, partner, title, asset_duration, tot_hov, tot_sessions, impressions, month, year_month_day, ref_id, content_provider, series, share, monthly_share, revenue, year, quarter, filename)
from (select 
            t.$1, 
            t.$2,
            t.$3,
            t.$4,
            t.$5, 
            t.$6,
            t.$7, 
            t.$8,
            t.$9,
            t.$10,
            t.$11,
            t.$12,
            t.$13, 
            to_decimal(REPLACE(t.$14, '%'), 11, 8),
            to_decimal(REPLACE(t.$15, '%'), 6, 3),
            to_number(REPLACE(REPLACE(t.$16, '$'), ','), 15, 6), 
            2022, 'q3', 'stirr_xiaomi_register_q3_2022.csv'
        from @distribution_partners t) pattern='.*stirr_xiaomi_register_q3_2022.*' file_format = nosey_viewership 
        ON_ERROR=SKIP_FILE FORCE=TRUE;    
        
        
        
copy into AMAGI_VIEWERSHIP(
platform_content_name,
tot_hov,
channel, 
month,
year_month_day,
ref_id,
content_provider,
series,
share,
revenue,
year, 
quarter,
filename) 
from (
    select t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8,
    to_decimal(REPLACE(t.$9, '%'), 11, 8),
    to_number(REPLACE(t.$10, '$'), 15, 5),
    2022, 'q3', 'fubotv_register_q3_2022.csv'  from @distribution_partners t) pattern='.*fubotv_register_q3_2022.*' file_format = nosey_viewership 
ON_ERROR=SKIP_FILE;
        
        
        
-- Rlaxx (process invoice first)
copy into AMAGI_VIEWERSHIP(
channel,
platform_content_name,
revenue_euro,
content_provider,
series,
share,
revenue,
hov_share,
year, 
quarter,
channel_id,
territory,
deal_parent,
filename) 
from (
    select t.$1, t.$2, 
    to_number(REPLACE(REPLACE(t.$3, ','), '$'), 15, 5),
    t.$4, t.$5,
    to_decimal(REPLACE(t.$6, '%'), 11, 8),
    to_number(REPLACE(t.$7, '$'), 15, 5),
    t.$8,
    2022, 'q3',8, 'Europe', 22, 'rlaxx_register_q3_2022.csv'  from @distribution_partners t) pattern='.*rlaxx_register_q3_2022.*' file_format = nosey_viewership 
ON_ERROR=SKIP_FILE;
        
       
-- TCL       
copy into wurl_viewership (date, channel, partner, title, asset_duration, tot_hov, month, year_month_day, ref_id, content_provider, series, monthly_share, share, year, quarter, filename)
from (select 
            t.$1, 
            t.$2,
            t.$3,
            t.$4,
            t.$5, 
            t.$6,
            t.$7, 
            t.$8,
            t.$9,
            t.$10,
            t.$11,
            to_decimal(REPLACE(t.$12, '%'), 6, 3),
            to_decimal(REPLACE(t.$13, '%'), 11, 8),
            2022, 'q3', 'tcl_register_q3_2022.csv'
        from @distribution_partners t) pattern='.*tcl_register_q3_2022.*' file_format = nosey_viewership 
        ON_ERROR=SKIP_FILE FORCE=TRUE;
        
-- stremium
copy into AMAGI_VIEWERSHIP(
view_date,
platform_channel_name,
platform_content_id,
platform_content_name,
series,
sessions,	
tot_mov,
unique_viewers,
avg_session_count,
avg_duration_per_session,
avg_duration_per_viewer,
month,
year_month_day,
ref_id,
content_provider,
share,
year, 
quarter,
filename) 
from (
    select t.$1, t.$2, t.$3, t.$4, t.$5,
    to_number(REPLACE(t.$6, ','),12, 2),
    to_number(REPLACE(t.$7, ','), 12, 2),
    to_number(REPLACE(t.$8, ','), 12, 2),
    to_number(REPLACE(t.$9, ','), 8, 2),
    to_number(REPLACE(t.$10, ','), 8, 2),
    to_number(REPLACE(t.$11, ','), 8, 2), 
    t.$12, t.$13, t.$14, t.$15,
    to_number(REPLACE(t.$16, '%'), 11, 8),
    2022, 'q3', 'stremium_register_q3_2022.csv'  from @distribution_partners t) pattern='.*stremium_register_q3_2022.*' file_format = nosey_viewership 
ON_ERROR=SKIP_FILE;

-- Freebie 
copy into AMAGI_VIEWERSHIP(
series,
platform_content_name,
year_month_day,
views,
revenue,	
year,
quarter,
deal_parent,
filename) 
from (
    select 
    t.$1, 
    t.$1,
    t.$2,
    t.$3,
    to_number(REPLACE(t.$4, '$'), 15, 5),
    2022, 'q3', 27, 'freebie_register_q3_2022.csv'  from @distribution_partners t) pattern='.*freebie_register_q3_2022.*' file_format = nosey_viewership 
FORCE=TRUE, ON_ERROR=SKIP_FILE;






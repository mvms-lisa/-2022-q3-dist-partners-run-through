


-- Revenue Upload
        copy into revenue (year_month_day, title, impressions, cpms, amount, territory, territory_id, channel, channel_id, deal_parent, type, department, month, label, year, quarter, filename)
        from (
            select 
            t.$1, 
            t.$2,
            to_number(REPLACE(t.$3, ','), 16, 2),  
            to_number(REPLACE(t.$4, '$'), 8, 2),
            to_number(REPLACE(REPLACE(t.$5, '$'), ','), 12, 2), 
            t.$6,
            t.$7,
            t.$8,
            t.$9,
            t.$10,
            t.$11,
            t.$12,
            t.$13,
            'Revenue',
            2022,
            'q3',
            'revenue_register_q3_2022.csv'
        from @DISTRIBUTION_PARTNERS_REVENUE t) pattern='.*revenue_register_q3_2022.*' file_format = nosey_viewership 
        FORCE=TRUE ON_ERROR=SKIP_FILE;
        



-- WURL Q3 Expenses
-- WURL Copy into (NEW)
copy into expenses(
    year_month_day,
    description,
    quantity,
    rate,
    amount,
    title,
    deal_parent,
    department,
    channel,
    channel_id,
    territory,
    territory_id,
    month,
    type,
    adjustments,
    label,
    year,
    quarter,
    filename
    )
from (select t.$1,
      t.$2,
      to_number(REPLACE(REPLACE(t.$3, ','), '$'), 15, 4),  
      to_number(REPLACE(REPLACE(t.$4, ','), '$'), 10, 5),  
      to_number(REPLACE(REPLACE(t.$5, ','), '$'), 20, 5),  
       t.$6,
       t.$7,
       t.$8,
       t.$9,
       t.$10,
       t.$11,
       t.$12,
       t.$13,
      'Wurl', 'FALSE', 'Expense', 2022, 'q3', 'wurl_expenses_q3_2022.csv'
from @DISTRIBUTION_PARTNERS_EXPENSES t) pattern='.*wurl_expenses_q3_2022.*' file_format = nosey_viewership 
ON_ERROR=SKIP_FILE;

-- Expenses (Amagi - From Register)
copy into expenses(
    year_month_day,
    description,
    channel,
    channel_id,
    department,
    deal_parent,
    amount,
    territory,
    territory_id,
    title,
    month,
    label,
    type,
    year,
    quarter,
    filename
    )
from (select t.$1,
      t.$2,
      t.$3,
      t.$4,
      t.$5,
      t.$6,
      to_number(REPLACE(REPLACE(t.$7, ','), '$'), 15, 4),  
      t.$8,
      t.$9,
      t.$10,
      t.$11,
      'Expense','Amagi', 2022, 'q3', 'amagi_expenses_reg_q3_2022.csv'
from @DISTRIBUTION_PARTNERS_EXPENSES t) pattern='.*amagi_expenses_reg_q3_2022.*' file_format = nosey_viewership 
ON_ERROR=SKIP_FILE;
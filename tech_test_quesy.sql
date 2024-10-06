-- in the requirement is create one single query, so CTE is used here.
-- in the requirement, the output dataset need to return login/server/symbol/dt_report per day in 4 months 2020.
-- symbol_idx, date_idx, active_user are 3 tables that created for create index. 
-- concern: 1. it is good to have a source table for symbol instead of extracting from trades table which may be imcomplete.
-- 2. have checked login_hash and server_hash is 1 to 1 mapped, and same between trades and users tables. 
--    but the business logic might be different. in the query, assume the source of this column is coming from users table.
-- 3. records in the output dataset still nee to populated even there is no transactions.
-- 	  the output dataset rows would be count of days multiple count of users multiple count of symbols which is huge (more than 5 mil records).
--    and we need windows function to get the calculation done. populated dataset might need more resoure than micro/mini aws instance.
--    on my person computer which have 16 cores cpu and 64G memory, it need around 3 minutes to finish each run.
-- 4. not quite understand where the id column coming from in the output dataset. assume it is just a simple system id column.
-- 5. dt_report column is calculated based on close time which might be different business need.
-- 6. highlight: rank_volume_symbol_prev_7d need login/symbol rank_count_prev_7d need login only, others match the dataset which is login/server/symbol 


WITH SYMBOL_IDX AS ( -- create symbol index 
	SELECT SYMBOL
	FROM TRADES
	GROUP BY SYMBOL
), DATE_IDX AS ( -- create date index -- range 8 days earlier than 2020-06-01 to make sure the 'last 7 day' windows calculation correct.
	SELECT GENERATE_SERIES('2020-06-01'::DATE - INTERVAL '8 DAY', '2020-09-30'::DATE, '1 day'::INTERVAL)::DATE AS DATE
), ACTIVE_USER as ( -- create user index
	SELECT LOGIN_HASH, CURRENCY, SERVER_HASH
	FROM USERS
	WHERE ENABLE = 1
	GROUP BY LOGIN_HASH, CURRENCY, SERVER_HASH
), FULL_IDX AS ( -- combine 3 index table as full index 
    SELECT DATE AS DT_REPORT
        , LOGIN_HASH, CURRENCY, SYMBOL, SERVER_HASH
    FROM DATE_IDX D
    CROSS JOIN ACTIVE_USER U
    CROSS JOIN SYMBOL_IDX S
), TRADE_SUBSET AS ( -- aggregate trades table from trading transactions to reporting datetime level.
    SELECT CASE WHEN CLOSE_TIME::DATE <= '2020-06-01'::DATE - INTERVAL '8 DAY'
                    THEN '2020-06-01'::DATE - INTERVAL '8 DAY'
                ELSE CLOSE_TIME::DATE END AS DT_REPORT
        , LOGIN_HASH, SERVER_HASH, SYMBOL
        , SUM(VOLUME) AS VOLUME
        , COUNT(*) AS TRADE_COUNT -- could use ticket_hash as well.
    FROM TRADES
    GROUP BY CASE WHEN CLOSE_TIME::DATE <= '2020-06-01'::DATE - INTERVAL '8 DAY'
                    THEN '2020-06-01'::DATE - INTERVAL '8 DAY'
                ELSE CLOSE_TIME::DATE END, LOGIN_HASH, SERVER_HASH, SYMBOL
), TRADE_FULLSET AS ( -- get the primary table 
    SELECT I.DT_REPORT, I.LOGIN_HASH, I.SERVER_HASH, I.SYMBOL, I.CURRENCY
        , COALESCE(S.VOLUME, 0) AS VOLUME
        , COALESCE(S.TRADE_COUNT, 0 ) AS TRADE_COUNT
    FROM FULL_IDX I
    LEFT OUTER JOIN TRADE_SUBSET S
    ON I.DT_REPORT = S.DT_REPORT
        AND I.LOGIN_HASH = S.LOGIN_HASH
        AND I.SYMBOL = S.SYMBOL
), INIT_CALC AS ( -- get the first 3 columns which can be calculated by windows function directly.
    SELECT DT_REPORT, LOGIN_HASH, SERVER_HASH, SYMBOL, CURRENCY, VOLUME, TRADE_COUNT
        , SUM ( CASE WHEN DT_REPORT BETWEEN '2020-08-01' AND '2020-08-31' THEN VOLUME ELSE 0 END)
            OVER (PARTITION BY LOGIN_HASH, SERVER_HASH, SYMBOL ORDER BY DT_REPORT
                            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                           ) AS SUM_VOLUME_2020_08
        , SUM(VOLUME) OVER (PARTITION BY LOGIN_HASH, SERVER_HASH, SYMBOL ORDER BY DT_REPORT
                            RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW
                           ) AS SUM_VOLUME_PREV_7D
        , SUM(TRADE_COUNT) OVER (PARTITION BY LOGIN_HASH, SERVER_HASH, SYMBOL ORDER BY DT_REPORT
                            RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW
                           ) AS SUM_TRADE_PREV_7D
        , SUM(VOLUME) OVER (PARTITION BY LOGIN_HASH, SERVER_HASH, SYMBOL ORDER BY DT_REPORT
                            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                           ) AS SUM_VOLUME_PREV_ALL
    FROM TRADE_FULLSET
), RANK_CALC AS ( -- get the 2 additional ranking columns which is based on initial calculation.
    SELECT DT_REPORT, LOGIN_HASH, SERVER_HASH, SYMBOL, CURRENCY, VOLUME, TRADE_COUNT
        , SUM_VOLUME_PREV_7D, SUM_TRADE_PREV_7D, SUM_VOLUME_PREV_ALL, SUM_VOLUME_2020_08
        , DENSE_RANK() OVER (PARTITION BY LOGIN_HASH, SYMBOL ORDER BY SUM_VOLUME_PREV_7D DESC) AS RANK_VOLUME_SYMBOL_PREV_7D
        , DENSE_RANK() OVER (PARTITION BY LOGIN_HASH, SERVER_HASH, SYMBOL ORDER BY SUM_TRADE_PREV_7D DESC) AS RANK_COUNT_REV_7D
    FROM INIT_CALC
), FIRST_TRADE_DATE AS ( -- to get first trade date required one additional sub query, since it have different level of 'PK'.
    SELECT LOGIN_HASH, SERVER_HASH, SYMBOL
        , MIN(CLOSE_TIME)::DATE AS DATE_FIRST_TRADE
    FROM TRADES
    GROUP BY LOGIN_HASH, SERVER_HASH, SYMBOL
), OUTPUT AS (
    SELECT ROW_NUMBER() OVER () AS id
		, C.DT_REPORT, C.LOGIN_HASH, C.SERVER_HASH, C.SYMBOL, C.CURRENCY
        , C.SUM_VOLUME_PREV_7D, C.SUM_TRADE_PREV_7D, C.SUM_VOLUME_PREV_ALL, C.SUM_VOLUME_2020_08
		, C.RANK_VOLUME_SYMBOL_PREV_7D, C.RANK_COUNT_REV_7D, F.DATE_FIRST_TRADE
		, ROW_NUMBER() OVER ( ORDER BY C.DT_REPORT, C.LOGIN_HASH, C.SERVER_HASH, C.SYMBOL DESC)  AS ROW_NUMBER
	FROM RANK_CALC C
    LEFT OUTER JOIN FIRST_TRADE_DATE F
	ON C.LOGIN_HASH = F.LOGIN_HASH
		AND C.SERVER_HASH = F.SERVER_HASH
		AND C.SYMBOL = F.SYMBOL
	WHERE C.DT_REPORT >= '2020-06-01'::DATE -- REMOVE PREVIOUS DAYS RECORD TO MATCH THE FINAL OUTPUT
) SELECT * FROM OUTPUT
-- WHERE LOGIN_HASH = '18D4C2E739573770F9DF198F0E51C1B9' AND SYMBOL = 'AUDUSD' --test case 1 
-- WHERE LOGIN_HASH = '8FED54A1169EAB2C0DA69AD13CF66904' AND SYMBOL = 'USDCHF' --test case 2
-- ORDER BY LOGIN_HASH, SYMBOL, DT_REPORT
ORDER BY ROW_NUMBER DESC ;

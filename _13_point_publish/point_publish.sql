with cte as (
    SELECT
           history_type,
           total_point,
           free_point,
           cash_point,
           TIMEZONE('Asia/Seoul', TIMESTAMP WITH TIME ZONE 'epoch' + reg_time * INTERVAL '1 second') AS reg_time,
           TIMEZONE('Asia/Seoul', TIMESTAMP WITH TIME ZONE 'epoch' + reg_time * INTERVAL '1 second') AS expire_time
    FROM mkrs_schema.cms_mk_point_info_history
)
SELECT
       left(reg_time, 7) AS "연월",
       history_type AS "CMS적립금코드",
       count(*) AS "지급코드건수",
       sum(total_point) AS "전체적립금",
       sum(free_point) AS "무상적립금",
       sum(cash_point) AS "유상적립금"
FROM cte
WHERE 1=1
  AND reg_time >= {{ params.start_date }}
  AND reg_time < {{ params.end_date }}
GROUP BY 1, 2
ORDER BY 1 ASC, 2 ASC;
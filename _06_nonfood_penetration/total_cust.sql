SELECT
  SUBSTRING(ord_dt, 1, 7) AS ord_ym,
  COUNT(DISTINCT cust_no) AS cust_n,
  SUM(gmv_retail) AS gmv,
  COUNT(DISTINCT ord_cd) AS ord_cnt
FROM mkrs_fa_schema.u_corpdev_ord_prd_1d
WHERE 1=1
  AND ord_dt >= {{ params.start_date }}
  AND ord_dt < {{ params.end_date }}
GROUP BY 1
SELECT
  SUBSTRING(ord_dt, 1, 7) AS ord_ym,
  COUNT(DISTINCT cust_no) AS cust_n,
  SUM(deal_tot_price) AS gmv,
  COUNT(DISTINCT ord_cd) AS ord_cnt
FROM mkrs_fa_schema.u_corp_ir_ord_prd_1m
WHERE 1=1
  AND ord_dt >= {{ params.start_date }}
  AND ord_dt < {{ params.end_date }}
GROUP BY 1
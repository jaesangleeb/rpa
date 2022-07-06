WITH cte AS (
    SELECT ord_dt,
           ord_cd,
           center_cd,
           cust_no,
           prd_tot_price,
           dc_point,
           dc_prd_tot,
           dc_prd_coupon,
           free_shipping,
           dlvy_cost,
           cnt,
           ROW_NUMBER() OVER (PARTITION BY ord_cd) AS row_no
    FROM mkrs_fa_schema.corp_ir_ord_prd_1m
    WHERE 1 = 1
      AND ord_dt >= {{ params.start_date }}
      AND ord_dt < {{ params.end_date }}
)
SELECT
  LEFT(ord_dt,7) AS ord_ym,
  center_cd,
  SUM(prd_tot_price) AS gmv,
  SUM(dc_prd_tot) AS dc_prd,
  SUM(dc_prd_coupon) AS dc_coupon,
  SUM(CASE WHEN row_no=1 THEN free_shipping ELSE 0 END) AS free_shipping,
  SUM(CASE WHEN row_no=1 THEN dc_point ELSE 0 END) AS dc_point,
  SUM(CASE WHEN row_no=1 THEN dlvy_cost ELSE 0 END) AS dlvy_cost,
  COUNT(DISTINCT ord_cd) AS ord_cnt,
  COUNT(DISTINCT cust_no) AS cust_cnt,
  SUM(cnt) as prd_cnt
FROM cte
GROUP BY LEFT(ord_dt,7), 2
ORDER BY center_cd;
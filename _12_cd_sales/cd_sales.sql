WITH cte AS (
    SELECT ord_dt,
           ord_cd,
           center_cd,
           cust_no,
           gmv_retail,
           dc_deal_tot,
           new_dc_deal_tot,
           dc_deal_coupon,
           dc_deal_point,
           free_shipping,
           dlvy_cost::INTEGER,
           cnt,
           ROW_NUMBER() OVER (PARTITION BY ord_cd) AS row_no
    FROM mkrs_fa_schema.u_corp_ir_ord_prd_1m
    WHERE 1 = 1
      AND ord_dt >= {{ params.start_date }}
      AND ord_dt < {{ params.end_date }}
)
SELECT
  LEFT(ord_dt,7) AS ord_ym,
  center_cd,
  SUM(gmv_retail) AS gmv,
  SUM(dc_deal_tot) AS dc_deal_tot,
  SUM(dc_deal_coupon) AS dc_deal_coupon,
  SUM(CASE WHEN row_no=1 THEN free_shipping ELSE 0 END) AS free_shipping,
  SUM(dc_deal_point) AS dc_deal_point,
  SUM(new_dc_deal_tot) AS new_dc_deal_tot,
  SUM(CASE WHEN row_no=1 THEN dlvy_cost ELSE 0 END) AS dlvy_cost,
  COUNT(DISTINCT ord_cd) AS ord_cnt,
  COUNT(DISTINCT cust_no) AS cust_cnt,
  SUM(cnt) as prd_cnt
FROM cte
GROUP BY LEFT(ord_dt,7), 2
ORDER BY center_cd;
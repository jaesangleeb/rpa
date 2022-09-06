WITH cte AS (
    SELECT ord_dt,
           ord_cd,
           center_cd,
           cust_no,
           gmv_retail,
           deal_tot_price,
           dc_deal_tot,
           new_dc_deal_tot,
           dc_deal_coupon,
           dc_deal_point,
           free_shipping,
           dlvy_cost::INTEGER,
           cnt,
           dc_point,
           ROW_NUMBER() OVER (PARTITION BY ord_cd) AS row_no
    FROM mkrs_fa_schema.u_corpdev_ord_prd_1d
    WHERE 1 = 1
      AND ord_dt >= {{ params.start_date }}
      AND ord_dt < {{ params.end_date }}
      AND deal_status < 40
)
SELECT
  LEFT(ord_dt,7) AS ord_ym,
  center_cd,
  SUM(gmv_retail) AS gmv1,
  SUM(deal_tot_price) AS gmv2,
  SUM(dc_deal_tot) AS dc_deal_tot,
  SUM(dc_deal_coupon) AS dc_deal_coupon,
  SUM(CASE WHEN row_no=1 THEN free_shipping ELSE 0 END) AS free_shipping,
  SUM(CASE WHEN row_no=1 THEN dc_point ELSE 0 END) AS dc_point,
  SUM(new_dc_deal_tot) AS new_dc_deal_tot,
  SUM(CASE WHEN row_no=1 THEN dlvy_cost ELSE 0 END) AS dlvy_cost,
  COUNT(DISTINCT ord_cd) AS ord_cnt,
  COUNT(DISTINCT cust_no) AS cust_cnt,
  SUM(cnt) as prd_cnt
FROM cte
GROUP BY LEFT(ord_dt,7), 2
ORDER BY center_cd;
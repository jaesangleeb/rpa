SELECT
       LEFT(ord_dt, 7) AS ord_ym,
       catg_1_nm,
       catg_2_nm,
       COUNT(DISTINCT ord_cd) AS ord_cnt,
       COUNT(DISTINCT cust_no) AS purchaser,
       SUM(prd_tot_price) AS gmv,
       SUM(dc_prd_tot) AS dc_prd,
       SUM(dc_prd_coupon) AS dc_coupon,
       SUM(using_point) AS dc_point
FROM mkrs_fa_schema.corp_ir_ord_prd_1m
WHERE 1=1
  AND catg_1_nm = '뷰티'
  AND ord_dt >= {{ params.start_date }}
  AND ord_dt < {{ params.end_date }}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
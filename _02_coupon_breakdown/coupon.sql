WITH ordprd AS (
         SELECT
             ord_dt,
             ord_cd,
             coupon_name,
             coupon_description,
             ptype,
             master_cd,
             publish_id,
             meta_id,
             dc_deal_coupon,
             free_shipping,
             center_cd,
             benefit_type,
             reason1,
             reason2,
             sharing_ratio,
             CASE WHEN benefit_type = 'FREE_SHIPPING' THEN
                 (cnt / SUM(cnt) OVER (PARTITION BY ord_cd)::float8)*3000
                  ELSE 0 END AS free_shipping_distributed
         FROM mkrs_fa_schema.u_corp_ir_ord_prd_1m
         WHERE 1=1
           AND ord_dt >= {{ params.start_date }}
           AND ord_dt < {{ params.end_date }}
     ),
     ord AS (
         SELECT
                ord_dt,
                ord_cd,
                coupon_name,
                coupon_description,
                ptype,
                publish_id,
                meta_id,
                SUM(dc_deal_coupon) AS dc_deal_coupon,
                free_shipping,
                center_cd,
                benefit_type,
                reason1,
                reason2,
                sharing_ratio,
                SUM(free_shipping_distributed)AS free_shipping_distributed
    FROM ordprd
    GROUP BY 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13
)
SELECT
       LEFT(ord_dt, 7) AS ord_ym,
       CASE WHEN coupon_name IS NULL THEN '더퍼플'
            ELSE coupon_name END AS coupon_name,
       coupon_description,
       ptype,
       publish_id,
       meta_id,
       SUM(CASE WHEN benefit_type = 'FREE_SHIPPING' THEN free_shipping_distributed
                ELSE dc_deal_coupon END)::float8 AS dc_deal_coupon,
       COUNT(DISTINCT ord_cd) AS ord_cnt,
       center_cd,
       CASE WHEN benefit_type IS NULL THEN 'PERCENT_DISCOUNT'
            ELSE benefit_type END AS benefit_type,
       CASE WHEN coupon_name LIKE '%%샛별%%' THEN 'CRM'
            WHEN coupon_name LIKE '%%브로셔%%' THEN 'CRM'
            WHEN coupon_name LIKE '%%고객%%감사%%' THEN '기타'
            WHEN reason1 IS NULL THEN 'CRM'
            ELSE reason1 END AS reason1,
       CASE WHEN coupon_name LIKE '%%샛별%%' THEN '기타'
            WHEN coupon_name LIKE '%%브로셔%%' THEN '초회차'
            WHEN coupon_name LIKE '%%고객%%감사%%' THEN '기타'
            WHEN reason2 IS NULL THEN '러버스'
            ELSE reason2 END AS reason2,
       sharing_ratio
FROM ord
GROUP BY 1, 2, 3, 4, 5, 8, 9, 10, 11, 12
ORDER BY 1 DESC;
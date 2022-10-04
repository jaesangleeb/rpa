WITH thirdparty AS
         (
             SELECT ord_cd
             FROM mkrs_fa_schema.u_corpdev_ord_prd_1d
             WHERE 1 = 1
               AND ptype = '3p'
               AND ord_dt >= {{ params.start_date }}
               AND ord_dt < {{ params.end_date }}
               AND deal_status < 40
             GROUP BY 1
         ),
     thirdparty2 AS (
         SELECT group_ord_cd
         FROM mkrs_fa_schema.u_corpdev_ord_prd_1d
         WHERE 1 = 1
           AND ord_cd IN (SELECT * FROM thirdparty)
           AND deal_status < 40
         GROUP BY 1
     ),
     thirdparty3 AS(
         SELECT
                *,
                CASE WHEN ptype = '3p' THEN 1 ELSE 0 END AS ptype_yn
         FROM mkrs_fa_schema.u_corpdev_ord_prd_1d
         WHERE 1 = 1
           AND group_ord_cd IN (SELECT * FROM thirdparty2)
           AND deal_status < 40
     ),
     only_sool AS(
         SELECT
                group_ord_cd
         FROM thirdparty3
         GROUP BY 1
         HAVING SUM(ptype_yn) = COUNT(ord_cd)
     ),
     mixed_sool AS(
         SELECT
                group_ord_cd
         FROM thirdparty3
         GROUP BY 1
         HAVING SUM(ptype_yn) != COUNT(ord_cd)
     ),
     fin AS(
         (SELECT *, 'ONLY' AS mixed_yn FROM thirdparty3 WHERE group_ord_cd IN (SELECT * FROM only_sool))
         UNION ALL
         (SELECT *, 'MIXED' AS mixed_yn FROM thirdparty3 WHERE group_ord_cd IN (SELECT * FROM mixed_sool))
     )
SELECT
       LEFT(ord_dt, 7) AS ord_ym,
       mixed_yn,
       ptype,
       catg_1_nm,
       catg_2_nm,
       COUNT(DISTINCT group_ord_cd) AS ord_cnt,
       COUNT(DISTINCT cust_no) AS purchasers,
       SUM(gmv_retail) AS gmv1,
       SUM(deal_tot_price) AS gmv2,
       SUM(dc_deal_tot) AS dc_deal_tot,
       SUM(dc_deal_coupon) AS dc_deal_coupon,
       SUM(dc_deal_point) AS dc_deal_point
FROM fin
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
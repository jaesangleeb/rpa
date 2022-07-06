(
SELECT
       LEFT(ord_dt, 7) as ord_ym,
       COALESCE(sd_nm, '지역정보없음') AS sd_nm,
       COALESCE(sgg_nm, '지역정보없음') AS sgg_nm,
       '전체' as dlvy_type,
       COUNT(DISTINCT ord_cd) AS ord_cnt,
       COUNT(DISTINCT cust_no) AS cust_cnt,
       SUM(ord_pay) AS ord_pay,
       SUM(gmv_excltax) AS gmv_excltax,
       SUM(dc_prd_excltax) AS dc_prd_excltax,
       SUM(dc_coupon_excltax) AS dc_coupon_excltax,
       SUM(dc_point_excltax) AS dc_point_excltax,
       SUM(gmv_excltax) - SUM(dc_prd_excltax) - SUM(dc_coupon_excltax) - SUM(dc_point_excltax) AS partial_em_excltax,
       SUM(gmv) AS gmv,
       SUM(dc_prd) AS dc_prd,
       SUM(dc_coupon) AS dc_coupon,
       SUM(dc_point) AS dc_point,
       SUM(gmv) - SUM(dc_prd) - SUM(dc_coupon) - SUM(dc_point) AS partial_em,
       COUNT(DISTINCT manager_cd || '_' || manager_nm) / COUNT (DISTINCT LEFT(ord_dt, 10))::FLOAT AS mngr_avg
FROM mkrs_fa_schema.cd_topline_region_1m
where 1=1
and ord_dt >= {{ params.start_date }}
and ord_dt < {{ params.end_date }}
GROUP BY 1, 2, 3, 4
)
union all
(
SELECT
       LEFT(ord_dt, 7) as ord_ym,
       COALESCE(sd_nm, '지역정보없음') AS sd_nm,
       COALESCE(sgg_nm, '지역정보없음') AS sgg_nm,
       CASE WHEN dlvy_type LIKE '%%DAWN%%' THEN '샛별'
            WHEN dlvy_type LIKE '%%PARCEL%%' THEN '택배'
            WHEN dlvy_type IS NULL THEN '지역정보없음'
            ELSE NULL END AS dlvy_type,
       COUNT(DISTINCT ord_cd) AS ord_cnt,
       COUNT(DISTINCT cust_no) AS cust_cnt,
       SUM(ord_pay) AS ord_pay,
       SUM(gmv_excltax) AS gmv_excltax,
       SUM(dc_prd_excltax) AS dc_prd_excltax,
       SUM(dc_coupon_excltax) AS dc_coupon_excltax,
       SUM(dc_point_excltax) AS dc_point_excltax,
       SUM(gmv_excltax) - SUM(dc_prd_excltax) - SUM(dc_coupon_excltax) - SUM(dc_point_excltax) AS partial_em_excltax,
       SUM(gmv) AS gmv,
       SUM(dc_prd) AS dc_prd,
       SUM(dc_coupon) AS dc_coupon,
       SUM(dc_point) AS dc_point,
       SUM(gmv) - SUM(dc_prd) - SUM(dc_coupon) - SUM(dc_point) AS partial_em,
       COUNT(DISTINCT manager_cd || '_' || manager_nm) / COUNT (DISTINCT LEFT(ord_dt, 10))::FLOAT AS mngr_avg
FROM mkrs_fa_schema.cd_topline_region_1m
where 1=1
and ord_dt >= {{ params.start_date }}
and ord_dt < {{ params.end_date }}
GROUP BY 1, 2, 3, 4
) ORDER BY 1, 2, 3, 4;
WITH cte AS (
    SELECT LEFT(ord_dt, 10) AS ord_dt,
            ir.ord_cd,
            ir.center_cd,
            ir.prd_cd,
            ir.prd_nm,
            ir.cnt,
            ir.prd_tot_price,
            ir.dc_prd_tot,
            ir.dc_prd_coupon,
            ir.using_point,
            ir.ptype,
            CASE
                 WHEN ir.prd_nm LIKE '%%전통주%%' THEN '전통주'
                 WHEN ir.prd_nm LIKE '%%예약딜리버리%%' THEN '예약딜리버리'
                 WHEN ir.prd_nm LIKE '%%항공권%%' THEN '항공권'
                 WHEN ir.prd_nm LIKE '%%입장권%%' THEN '입장권'
                 WHEN ir.prd_nm LIKE '%%설치배송%%' THEN '설치배송'
                 WHEN ir.prd_nm LIKE '%%숙박권%%' THEN '숙박권'
                 WHEN ir.prd_nm LIKE '%%오픈갤러리%%' THEN '오픈갤러리'
                 WHEN ir.prd_nm LIKE '%%블루독%%택배수령%%' THEN '블루독'
                 WHEN ir.prd_nm LIKE '%%식사권%%' THEN '식사권'
                 WHEN ir.prd_nm LIKE '%%밍크뮤%%' THEN '밍크뮤'
                 WHEN ir.prd_nm LIKE '%%셀프픽업%%' THEN '셀프픽업'
                 WHEN ir.prd_nm LIKE '%%업체배송%%' THEN '업체배송'
                 WHEN ir.prd_nm LIKE '%%택배수령%%' THEN '택배수령'
                 WHEN ir.prd_nm SIMILAR TO '%%해외여행%%|%%국내여행%%' THEN '여행'
                 ELSE NULL END AS gubn,
-- '22년 5월은 MD분들이 문자열 반대순서로 넣음
--             CASE WHEN ir.prd_nm LIKE '%%설치배송%%' THEN SPLIT_PART(SPLIT_PART(ir.prd_nm, ']', 1), '[', 2)
--                  ELSE gubn END AS gubn_detail
            CASE WHEN ir.prd_nm LIKE '%%설치배송%%' THEN SPLIT_PART(SPLIT_PART(prd_nm, ']', 2), '[', 2)
                 ELSE gubn END AS gubn_detail
     FROM mkrs_fa_schema.corp_ir_ord_prd_1m ir
     WHERE 1 = 1
       AND ir.ptype = '3p'
       AND LEFT(ord_dt, 10) >= {{ params.start_date }}
       AND LEFT(ord_dt, 10) < {{ params.end_date }}
        ),
     prdinfo AS (
SELECT
    pid.prd_no,
    pid.prd_cd,
    gid.catg_1_nm,
    gid.catg_2_nm,
    gid.catg_3_nm,
    pid.prd_nm,
    gid.catg_1_cd,
    gid.catg_2_cd,
    gid.catg_3_cd
FROM (
      SELECT
        prd_no, prd_cd, prd_nm
      FROM mkrs_aa_schema.prd_info_option_1d
      GROUP BY prd_no, prd_cd, prd_nm
      ) pid
INNER JOIN mkrs_aa_schema.goods_info_1d gid
  ON pid.prd_cd = gid.prd_cd
GROUP BY 1,2,3,4,5,6,7,8,9
         )
SELECT
       cte.ord_dt,
       cte.center_cd,
       cte.prd_cd,
       cte.prd_nm,
       prdinfo.catg_1_nm,
       COUNT(DISTINCT cte.ord_cd) AS ord_cnt,
       SUM(cte.cnt) AS cnt,
       SUM(cte.prd_tot_price) AS gmv,
       SUM(cte.dc_prd_tot) AS dc,
       SUM(cte.dc_prd_coupon) AS coupon,
       SUM(cte.using_point) AS using_point,
       cte.ptype,
       gubn,
       gubn_detail
--        CASE WHEN prdinfo.catg_1_nm LIKE '%%여행/문화/서비스%%' THEN '여행'
--        ELSE gubn END AS gubn,
--        CASE WHEN prdinfo.catg_1_nm LIKE '%%여행/문화/서비스%%' THEN '여행'
--        ELSE gubn_detail END AS gubn_detail
FROM cte
LEFT JOIN prdinfo
    ON cte.prd_cd = prdinfo.prd_cd
GROUP BY 1, 2, 3, 4, 5, 12, 13, 14
ORDER BY gubn, gubn_detail, ord_dt, center_cd, prd_nm;
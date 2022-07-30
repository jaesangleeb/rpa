WITH cte AS (
    SELECT LEFT (op.ord_dt, 10) AS ord_dt,
    op.ord_cd,
    op.center_cd,
    op.master_cd,
    op.master_nm,
    op.cnt,
    op.deal_tot_price,
    op.dc_deal_tot,
    op.dc_deal_coupon,
    op.dc_deal_point,
    op.ptype,
    CASE
    WHEN op.master_nm LIKE '%%전통주%%' THEN '전통주'
    WHEN op.master_nm LIKE '%%예약딜리버리%%' THEN '예약딜리버리'
    WHEN op.master_nm LIKE '%%항공권%%' THEN '항공권'
    WHEN op.master_nm LIKE '%%입장권%%' THEN '입장권'
    WHEN op.master_nm LIKE '%%설치배송%%' THEN '설치배송'
    WHEN op.master_nm LIKE '%%숙박권%%' THEN '숙박권'
    WHEN op.master_nm LIKE '%%오픈갤러리%%' THEN '오픈갤러리'
    WHEN op.master_nm LIKE '%%블루독%%택배수령%%' THEN '블루독'
    WHEN op.master_nm LIKE '%%식사권%%' THEN '식사권'
    WHEN op.master_nm LIKE '%%밍크뮤%%' THEN '밍크뮤'
    WHEN op.master_nm LIKE '%%셀프픽업%%' THEN '셀프픽업'
    WHEN op.master_nm LIKE '%%업체배송%%' THEN '업체배송'
    WHEN op.master_nm LIKE '%%택배수령%%' THEN '택배수령'
    WHEN op.master_nm SIMILAR TO '%%해외여행%%|%%국내여행%%' THEN '여행'
    ELSE NULL END AS gubn,
-- '22년 5월은 MD분들이 문자열 반대순서로 넣음
--             CASE WHEN ir.prd_nm LIKE '%%설치배송%%' THEN SPLIT_PART(SPLIT_PART(ir.prd_nm, ']', 1), '[', 2)
--                  ELSE gubn END AS gubn_detail
    CASE WHEN op.master_nm LIKE '%%설치배송%%' THEN SPLIT_PART(SPLIT_PART(op.master_nm, ']', 2), '[', 2)
    ELSE gubn END AS gubn_detail
    catg_1_nm
    FROM mkrs_fa_schema.u_corp_ir_ord_prd_1m op
    WHERE 1 = 1
       AND op.ptype = '3p'
       AND LEFT(op.ord_dt, 10) >= {{ params.start_date }}
       AND LEFT(op.ord_dt, 10) < {{ params.end_date }}
)
SELECT
       cte.ord_dt,
       cte.center_cd,
       cte.master_cd,
       cte.master_nm,
       cte.catg_1_nm,
       COUNT(DISTINCT cte.ord_cd) AS ord_cnt,
       SUM(cte.cnt) AS cnt,
       SUM(cte.deal_tot_price) AS gmv,
       SUM(cte.dc_deal_tot) AS dc,
       SUM(cte.dc_deal_coupon) AS coupon,
       SUM(cte.dc_deal_point) AS using_point,
       cte.ptype,
       gubn,
       gubn_detail
--        CASE WHEN prdinfo.catg_1_nm LIKE '%%여행/문화/서비스%%' THEN '여행'
--        ELSE gubn END AS gubn,
--        CASE WHEN prdinfo.catg_1_nm LIKE '%%여행/문화/서비스%%' THEN '여행'
--        ELSE gubn_detail END AS gubn_detail
FROM cte
GROUP BY 1, 2, 3, 4, 5, 12, 13, 14
ORDER BY gubn, gubn_detail, ord_dt, center_cd, prd_nm;
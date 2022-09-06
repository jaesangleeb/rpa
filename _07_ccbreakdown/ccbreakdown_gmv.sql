
(SELECT
    LEFT(ord_dt, 7) AS ord_ym,
    'TOTAL' AS feature,
    center_cd,
    SUM(gmv_retail) AS GMV
FROM mkrs_fa_schema.u_corpdev_ord_prd_1d
WHERE 1=1
  AND ord_dt >= {{ params.start_date }}
  AND ord_dt < {{ params.end_date }}
  AND deal_status < 40
GROUP BY 1, 2, 3)
UNION ALL
(SELECT
    LEFT(ord_dt, 7) AS ord_ym,
    '1p',
    center_cd,
    SUM(gmv_retail) AS GMV
FROM mkrs_fa_schema.u_corpdev_ord_prd_1d
WHERE 1=1
  AND ord_dt >= {{ params.start_date }}
  AND ord_dt < {{ params.end_date }}
  AND ptype = '1p'
  AND deal_status < 40
GROUP BY 1, 2, 3)
UNION ALL
(SELECT
    LEFT(ord_dt, 7) AS ord_ym,
    '3p',
    center_cd,
    SUM(gmv_retail) AS GMV
FROM mkrs_fa_schema.u_corpdev_ord_prd_1d
WHERE 1=1
  AND ord_dt >= {{ params.start_date }}
  AND ord_dt < {{ params.end_date }}
  AND ptype = '3p'
  AND deal_status < 40
GROUP BY 1, 2, 3)
UNION ALL
(SELECT
    LEFT(ord_dt, 7) AS ord_ym,
    'Beauty',
    center_cd,
    SUM(gmv_retail) AS GMV
FROM mkrs_fa_schema.u_corpdev_ord_prd_1d
WHERE 1=1
  AND ord_dt >= {{ params.start_date }}
  AND ord_dt < {{ params.end_date }}
  AND catg_1_nm like '%%뷰티%%'
  AND deal_status < 40
GROUP BY 1, 2, 3)

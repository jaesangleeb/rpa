
(SELECT
    LEFT(ord_dt, 7) AS ord_ym,
    'TOTAL' AS feature,
    center_cd,
    SUM(prd_tot_price) AS GMV
FROM mkrs_fa_schema.corp_ir_ord_prd_1m
WHERE 1=1
  AND ord_dt >= {{ params.start_date }}
  AND ord_dt < {{ params.end_date }}
GROUP BY 1, 2, 3)
UNION ALL
(SELECT
    LEFT(ord_dt, 7) AS ord_ym,
    '1p',
    center_cd,
    SUM(prd_tot_price) AS GMV
FROM mkrs_fa_schema.corp_ir_ord_prd_1m
WHERE 1=1
  AND ord_dt >= {{ params.start_date }}
  AND ord_dt < {{ params.end_date }}
  AND ptype = '1p'
GROUP BY 1, 2, 3)
UNION ALL
(SELECT
    LEFT(ord_dt, 7) AS ord_ym,
    '3p',
    center_cd,
    SUM(prd_tot_price) AS GMV
FROM mkrs_fa_schema.corp_ir_ord_prd_1m
WHERE 1=1
  AND ord_dt >= {{ params.start_date }}
  AND ord_dt < {{ params.end_date }}
  AND ptype = '3p'
GROUP BY 1, 2, 3)
UNION ALL
(SELECT
    LEFT(ord_dt, 7) AS ord_ym,
    'Beauty',
    center_cd,
    SUM(prd_tot_price) AS GMV
FROM mkrs_fa_schema.corp_ir_ord_prd_1m
WHERE 1=1
  AND ord_dt >= {{ params.start_date }}
  AND ord_dt < {{ params.end_date }}
  AND catg_1_nm like '%%뷰티%%'
GROUP BY 1, 2, 3)

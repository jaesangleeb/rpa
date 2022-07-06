WITH nonfood AS(
SELECT
    pio.prd_cd,
    pio.catg_1_nm,
    cmi.values_a
FROM mkrs_aa_schema.prd_info_option_1d pio
INNER JOIN mkrs_fa_schema.cd_master_info cmi
  ON pio.catg_1_nm LIKE '%%'||cmi.args_a||'%%' or (pio.catg_1_nm like '%%'||cmi.args_a||'%%' and pio.catg_1_nm like '%%'||cmi.args_b||'%%')
      -- args 증가시 추가
      --  or (pio.prd_nm like '%%'||cmi.args_a||'%%' and pio.prd_nm like '%%'||cmi.args_b||'%%')
WHERE cmi.keys = 'nonfood'
GROUP BY 1, 2, 3
)
SELECT
    LEFT(o.ord_dt, 7) AS ord_ym,
    o.prd_cd,
    o.prd_nm,
    o.catg_1_nm,
    o.catg_2_nm,
    COALESCE(n.values_a, 'food') AS nonfood,
    o.ptype,
    v.sourcing_type,
    v.kurly_keep_type,
    v.taxation,
    SUM(o.prd_tot_price) AS gmv,
    SUM(o.cnt) AS units
FROM mkrs_fa_schema.corp_ir_ord_prd_1m o
LEFT JOIN mkrs_schema.vendor_goods_master v
  ON o.prd_cd = v.goods_code
LEFT JOIN nonfood n
  ON o.prd_cd = n.prd_cd
WHERE 1=1
  AND o.ord_dt >= {{ params.start_date }}
  AND o.ord_dt < {{ params.end_date }}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;
WITH nonfood AS(
SELECT
    pio.master_cd,
    pio.catg_1_nm,
    cmi.values_a
FROM mkrs_aa_schema.u_prd_info_1d pio
INNER JOIN mkrs_fa_schema.cd_master_info cmi
  ON pio.catg_1_nm LIKE '%%'||cmi.args_a||'%%' or (pio.catg_1_nm like '%%'||cmi.args_a||'%%' and pio.catg_1_nm like '%%'||cmi.args_b||'%%')
      -- args 증가시 추가
      --  or (pio.prd_nm like '%%'||cmi.args_a||'%%' and pio.prd_nm like '%%'||cmi.args_b||'%%')
WHERE cmi.keys = 'nonfood'
GROUP BY 1, 2, 3
)
SELECT
    LEFT(o.ord_dt, 7) AS ord_ym,
    o.master_cd,
    o.master_nm,
    o.catg_1_nm,
    o.catg_2_nm,
    COALESCE(n.values_a, 'food') AS nonfood,
    o.ptype,
    o.sourcing_type,
    o.strage_type,
    o.tax
    SUM(o.deal_tot_price) AS gmv,
    SUM(o.dc_deal_tot) AS gmv,
    SUM(o.new_dc_deal_tot) AS gmv,
    SUM(o.dc_deal_coupon) AS gmv,
    SUM(o.dc_deal_point) AS gmv,
    SUM(o.cnt) AS units
FROM mkrs_fa_schema.u_corp_ir_ord_prd_1m o
LEFT JOIN mkrs_schema.vendor_goods_master v
  ON o.master_cd = v.goods_code
LEFT JOIN nonfood n
  ON o.master_cd = n.master_cd
WHERE 1=1
  AND o.ord_dt >= {{ params.start_date }}
  AND o.ord_dt < {{ params.end_date }}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;
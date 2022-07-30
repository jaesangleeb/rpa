WITH dlvy_3p AS(
  SELECT
    pio.prd_cd,
    pio.prd_nm,
    cmi.args_a
  FROM mkrs_aa_schema.prd_info_option_1d pio
  INNER JOIN mkrs_fa_schema.cd_master_info cmi
    ON pio.prd_nm LIKE '%%'||cmi.args_a||'%%'
      or (pio.prd_nm like '%%'||cmi.args_a||'%%' and pio.prd_nm like '%%'||cmi.args_b||'%%')
      -- args 증가시 추가
      --  or (pio.prd_nm like '%%'||cmi.args_a||'%%' and pio.prd_nm like '%%'||cmi.args_b||'%%')
  WHERE cmi.keys = 'dlvy_3p'
)
SELECT
       o.*,
       LEFT(ord_dt, 7) AS ord_ym,
       CASE WHEN o.ptype = '3p' THEN 1
       ELSE 0 END AS yn_3p,
       CASE WHEN d.prd_cd IS NOT NULL THEN d.args_a
       ELSE '기타' END AS dlvy_yn_3p,
       1 AS sku
FROM mkrs_fa_schema.u_corp_ir_ord_prd_1m o
LEFT JOIN dlvy_3p d
  ON o.prd_cd = d.prd_cd
WHERE 1=1
  AND ord_cd IN
      (
          SELECT ord_cd
          FROM mkrs_fa_schema.corp_ir_ord_prd_1m
          WHERE 1 = 1
            AND ord_dt >= {{ params.start_date }}
            AND ord_dt < {{ params.end_date }}
            AND ptype = '3p'
            AND prd_status < 40
          GROUP BY 1
      );
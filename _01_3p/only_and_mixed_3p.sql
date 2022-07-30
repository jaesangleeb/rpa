SELECT *
FROM mkrs_fa_schema.u_corp_ir_ord_prd_1m
WHERE ord_cd IN (
    SELECT ord_cd
    FROM mkrs_fa_schema.u_corp_ir_ord_prd_1m
    WHERE 1=1
      AND ord_dt >= {{ params.start_date }}
      AND ord_dt < {{ params.end_date }}
      AND ptype = '3p'
--       AND prd_nm LIKE '%%전통주%%'
      );
SELECT SUBSTRING(ord_dt, 1, 7) AS ord_ym,
       catg_1_nm,
       COUNT(DISTINCT cust_no) AS cust_n,
       SUM(deal_tot_price)      AS gmv,
       COUNT(DISTINCT ord_cd)  AS ord_cnt
FROM mkrs_fa_schema.u_corp_ir_ord_prd_1m
WHERE 1 = 1
  AND ord_dt >= {{ params.start_date }}
  AND ord_dt < {{ params.end_date }}
  AND deal_status < 40
  AND catg_1_nm in ('가전제품'
    , '반려동물'
    , '뷰티'
    , '생활용품'
    , '생활잡화'
    , '여행/문화/서비스'
    , '유아동'
    , '주방용품'
    , '가구/인테리어'
    , '패션/잡화'
    , '스포츠/레저'
    , '기타'
    , '헬스')
GROUP BY 1, 2
ORDER BY 1;
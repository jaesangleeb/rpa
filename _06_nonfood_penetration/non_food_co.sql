SELECT SUBSTRING(ord_dt, 1, 7)   ord_ym,
       COUNT(DISTINCT cust_no)   cust_n,
       SUM(gmv_retail)     AS gmv,
       COUNT(DISTINCT ord_cd) AS ord_cnt
FROM mkrs_fa_schema.u_corpdev_ord_prd_1d
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
    , '헬스'
    , '기타')
GROUP BY 1
ORDER BY 1;
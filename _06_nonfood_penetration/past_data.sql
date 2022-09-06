-- catg_non_food_co.sql
SELECT SUBSTRING(o.ord_dt, 1, 7) AS ord_ym,
       p.catg_1_nm,
       COUNT(DISTINCT o.cust_no) AS cust_n,
       SUM(o.prd_tot_price)      AS gmv,
       COUNT(DISTINCT o.ord_cd)  AS ord_cnt
FROM mkrs_aa_schema.ord_prd_10mm o
LEFT JOIN mkrs_aa_schema.prd_info_option_1d p
  ON o.prd_no = p.prd_no
WHERE 1 = 1
  AND ord_dt >= '2017-01-01'
  AND ord_dt < '2022-01-01'
  AND prd_status
    < 40
  AND catg_1_nm IN
--여기부터 카테고리 커스터마이징 하셈!
     ('가전제품'
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
GROUP BY 1, 2
ORDER BY 1;

-- non_food_co.sql
SELECT SUBSTRING(o.ord_dt, 1, 7) AS ord_ym,
       COUNT(DISTINCT o.cust_no) AS cust_n,
       SUM(o.prd_tot_price)      AS gmv,
       COUNT(DISTINCT o.ord_cd)  AS ord_cnt
FROM mkrs_aa_schema.ord_prd_10mm o
LEFT JOIN mkrs_aa_schema.prd_info_option_1d p
  ON o.prd_no = p.prd_no
WHERE 1 = 1
  AND ord_dt >= '2017-01-01'
  AND ord_dt < '2022-01-01'
  AND prd_status
    < 40
  AND catg_1_nm IN
--여기부터 카테고리 커스터마이징 하셈!
     ('가전제품'
    , '반려동물'
    , '뷰티'
    , '생활용품'
    , '생활잡화'
    , '여행/문화/서비스'
    , '유아동'
    , '주방용품'
    , '가구/인테리어'
    , '패션/잡화'
    , '스포츠/레저')
GROUP BY 1
ORDER BY 1;

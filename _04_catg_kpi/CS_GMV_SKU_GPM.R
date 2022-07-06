install.packages("httr")
install.packages("Rcpp")
install.packages("glue")
install.packages("redshiftTools")
install.packages("RPostgreSQL")

library(redshiftTools)
library(RPostgreSQL)
library(tidyverse)

redshift_con <- dbConnect(PostgreSQL(), 
                          host="mk-rs-cluster01.cohm4jocn4v7.ap-northeast-2.redshift.amazonaws.com", 
                          dbname="mkrsdb", 
                          user="jaesanglee_b", 
                          password="Jaesanglee_b!)06", 
                          port="5439")

# 1. GMV-SKU by Category(PB vs Non-PB) Historical

start_dt <- as.Date('2021-10-01')
end_dt <- as.Date('2021-11-30')



prd_info <- dbGetQuery(redshift_con,
                                paste0("select distinct pi.prd_cd, pi.prd_nm, pi.catg_1_nm, pi.catg_2_nm, pi.catg_3_nm
                             from mkrs_aa_schema.prd_info_1d pi
                             inner join (select pi.prd_cd, max(pi.update_dt) as update_dt
                                         from mkrs_aa_schema.prd_info_1d pi
                                         group by pi.prd_cd) pi2 on pi.prd_cd = pi2.prd_cd and pi.update_dt = pi2.update_dt
                             where not pi.prd_cd = ''")) %>% tbl_df() %>% distinct()

sourcing_type <- dbGetQuery(redshift_con,
                                     paste0("
                                            select distinct goods_code prd_cd, sourcing_type
                                            from mkrs_schema.vendor_goods_master
                                            ")) %>% tbl_df() %>% distinct()
          

ord_prd_m <- dbGetQuery(redshift_con,
                    paste0("
                           select substring(ord_ymd, 1, 7) ord_ym, prd_no, prd_cd, sum(sales) sales, sum(cnt) cnt
                           from mkrs_aa_schema.s_prd_sales_daily_1d
                           where ord_ymd >= '",start_dt,"' and ord_ymd < '",end_dt+1,"'
                           group by 1,2,3")) %>% tbl_df() %>% distinct() %>% 
                           left_join(prd_info, by = 'prd_cd') %>% left_join(sourcing_type, by = 'prd_cd') %>% 
                           select(ord_ym, prd_no, prd_cd, prd_nm, catg_1_nm, catg_2_nm, catg_3_nm, sales, cnt, sourcing_type)

ord_prd_y <- dbGetQuery(redshift_con,
                                 paste0("
                                         select substring(ord_ymd, 1, 4) ord_year, prd_no, prd_cd, sum(sales) sales, sum(cnt) cnt
                                         from mkrs_aa_schema.s_prd_sales_daily_1d
                                         where ord_ymd >= '2017-01-01' and ord_ymd < '",end_dt+1,"'
                                         group by 1,2,3")) %>% tbl_df() %>% distinct() %>% 
                                         left_join(prd_info, by = 'prd_cd') %>% left_join(sourcing_type, by = 'prd_cd') %>% 
                                         select(ord_year, prd_no, prd_cd, prd_nm, catg_1_nm, catg_2_nm, catg_3_nm, sales, cnt, sourcing_type)


sheets <- list("월별_RAW" = ord_prd_m,
               "연도별_RAW" = ord_prd_y)


write_xlsx(sheets, "GMV-SKU Data.xlsx")


# 2. GP Margin by Category Historical

ord_prd_2 <- dbGetQuery(redshift_con,
                                 paste0("
                                         select s.ord_ymd, s.center_cd, s.prd_cd, s.catg_1_nm, sum(s.sales) gmv_inclvat, sum(s.cnt) cnt
                                         from mkrs_aa_schema.s_prd_sales_daily_1d s
                                         where ord_ymd >= '",start_dt,"' and ord_ymd < '",end_dt+1,"'
                                         group by 1,2,3,4")) %>% tbl_df() %>% distinct()


map <- dbGetQuery(redshift_con,
                          paste0("
                                 select date ord_ymd, cluster_center center_cd, product_code prd_cd, moving_average_price
                                 from mkrs_dataio_schema.commerce_moving_average_price
                                 where date >= '",start_dt,"' and date <= '",end_dt,"'
                                 ")) %>% tbl_df() %>% distinct()


tax_type <- dbGetQuery(redshift_con,
                                     paste0("
                                            select distinct goods_code prd_cd, taxation
                                            from mkrs_schema.vendor_goods_master
                                            ")) %>% tbl_df() %>% distinct()



ord_prd_gp <- ord_prd_2 %>% left_join(map, by = c('ord_ymd', 'center_cd', 'prd_cd')) %>% 
                            left_join(tax_type, by = 'prd_cd') %>% 
                            mutate(gmv_exvat = ifelse(taxation == "TAXED", gmv_inclvat/11*10, gmv_inclvat),
                                   sply_price_inclvat = ifelse(taxation == "TAXED", moving_average_price*cnt/10*11, moving_average_price*cnt),
                                   sply_price_exvat = moving_average_price*cnt,
                                   gross_profit = gmv_exvat - sply_price_exvat,
                                   ord_ym = substring(ord_ymd, 1,7)
                            ) %>% group_by(ord_ym, prd_cd, catg_1_nm, taxation) %>% summarise(
                              gmv_inclvat = sum(gmv_inclvat, na.rm = T),
                              gmv_exvat = sum(gmv_exvat, na.rm = T),
                              sply_price_inclvat = sum(sply_price_inclvat, na.rm =T),
                              sply_price_exvat = sum(sply_price_exvat, na.rm = T),
                              gross_profit = sum(gross_profit, na.rm = T)
                            ) %>% select(ord_ym, prd_cd, catg_1_nm, gmv_inclvat, gmv_exvat, sply_price_inclvat, sply_price_exvat, gross_profit, taxation)


write_xlsx(ord_prd_gp, "GP Margin Data.xlsx")

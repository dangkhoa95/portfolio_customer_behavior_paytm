-- USER SEGMENTATION (Percentile)

-- Step 1: Tính RFM cho từng khách hàng
WITH fact_table AS (
   SELECT transaction_id, customer_id, scenario_id, charged_amount, transaction_time, status_id
   FROM fact_transaction_2019
   UNION
   SELECT transaction_id, customer_id, scenario_id, charged_amount, transaction_time, status_id
   FROM fact_transaction_2020
)
, rfm_table AS (
   SELECT customer_id
       , DATEDIFF ( day, MAX (transaction_time ), '2020-12-31') AS recency -- khoảng cách từ lần cuối so với ngày 31-12-2020
       , COUNT ( DISTINCT CONVERT (varchar(10), transaction_time) ) AS frequency -- đếm số ngày thanh toán thành công
       , SUM (charged_amount*1.0) AS monetary -- tính tổng tiền
   FROM fact_table
   LEFT JOIN dim_scenario scena
       ON fact_table.scenario_id = scena.scenario_id
   WHERE sub_category = 'Telco Card' AND status_id = 1
   GROUP BY customer_id
) -- b2: đánh thứ hạng r,f,m theo %
--> PERCENT_RANK(): đánh thứ hạng theo % (percentile)
, rank_table AS (
   SELECT *
       , PERCENT_RANK() OVER ( ORDER BY recency ASC ) AS r_rank
       , PERCENT_RANK() OVER ( ORDER BY frequency DESC ) AS f_rank
       , PERCENT_RANK() OVER ( ORDER BY monetary DESC ) AS m_rank
   FROM rfm_table
) -- chia tier
, tier_table AS (
   SELECT *
       , CASE WHEN r_rank > 0.75 THEN 4
           WHEN r_rank > 0.5 THEN 3
           WHEN r_rank > 0.25 THEN 2
           ELSE 1 END AS r_tier
       , CASE WHEN f_rank > 0.75 THEN 4
           WHEN f_rank > 0.5 THEN 3
           WHEN f_rank > 0.25 THEN 2
           ELSE 1 END AS f_tier
       , CASE WHEN m_rank > 0.75 THEN 4
           WHEN m_rank > 0.5 THEN 3
           WHEN m_rank > 0.25 THEN 2
           ELSE 1 END AS m_tier
   FROM rank_table
) -- ghep cac tier
, score_table AS (
   SELECT customer_id, recency, frequency, r_rank, f_rank, m_rank
       , CONCAT ( r_tier, f_tier, m_tier) AS rfm_score
   FROM tier_table
) -- phan loai KH
, segment_table AS (
   SELECT *
       , CASE
       WHEN rfm_score  =  111 THEN 'Best Customers' -- KH tốt nhất
       WHEN rfm_score LIKE '[3-4][3-4][1-4]' THEN 'Lost Bad Customer' -- KH rời bỏ mà còn siêu tệ (F thấp)
       WHEN rfm_score LIKE '[3-4]2[1-4]' THEN 'Lost Customers' -- KH cũng rời bỏ nhưng có valued (F = 3,4,5)
       WHEN rfm_score LIKE  '21[1-4]' THEN 'Almost Lost' -- sắp lost những KH này
       WHEN rfm_score LIKE  '11[2-4]' THEN 'Loyal Customers'
       WHEN rfm_score LIKE  '[1-2][1-3]1' THEN 'Big Spenders' -- chi nhiều tiền
       WHEN rfm_score LIKE  '[1-2]4[1-4]' THEN 'New Customers' -- KH mới nên là giao dịch ít
       WHEN rfm_score LIKE  '[3-4]1[1-4]' THEN 'Hibernating' -- ngủ đông (trc đó từng rất là tốt )
       WHEN rfm_score LIKE  '[1-2][2-3][2-4]' THEN 'Potential Loyalists' -- có tiềm năng
       ELSE 'unknown'
       END segment_label
   FROM score_table
)
SELECT segment_label
   , COUNT ( customer_id ) AS number_customer
   , SUM ( COUNT ( customer_id ) ) OVER () AS total_customer
   , FORMAT ( COUNT ( customer_id )*1.0 / SUM ( COUNT ( customer_id ) ) OVER () , 'p') AS pct
FROM segment_table
GROUP BY segment_label

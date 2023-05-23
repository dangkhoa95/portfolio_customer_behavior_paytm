-- RETENTION

-- Step 1: Xac dinh retained_users va pct

WITH subsequent_table AS (
	SELECT customer_id
		, MIN(transaction_time) OVER(PARTITION BY customer_id) first_trans
		, MONTH(MIN(transaction_time) OVER(PARTITION BY customer_id)) acquisition_month
		, DATEDIFF(month, MIN(transaction_time) OVER(PARTITION BY customer_id), transaction_time) subsequent_month
	FROM fact_transaction_2019 fact_19
	LEFT JOIN dim_scenario sce
		ON fact_19.scenario_id = sce.scenario_id
	WHERE sub_category = 'Telco Card' AND status_id = 1
)
, retained_table AS (
	SELECT acquisition_month, subsequent_month
		, COUNT(DISTINCT customer_id) retained_users
	FROM subsequent_table
	GROUP BY acquisition_month, subsequent_month
)
SELECT *
    , FIRST_VALUE(retained_users) OVER (PARTITION BY acquisition_month ORDER BY subsequent_month ASC) original_users
    , FORMAT(retained_users*1.0/FIRST_VALUE(retained_users) OVER (PARTITION BY acquisition_month ORDER BY subsequent_month ASC),'P') as pct_retained
FROM retained_table

-- Step 2: Dung pivot table

WITH subsequent_table AS (
	SELECT customer_id
		, MIN(transaction_time) OVER(PARTITION BY customer_id) first_trans
		, MONTH(MIN(transaction_time) OVER(PARTITION BY customer_id)) acquisition_month
		, DATEDIFF(month, MIN(transaction_time) OVER(PARTITION BY customer_id), transaction_time) subsequent_month
	FROM fact_transaction_2019 fact_19
	LEFT JOIN dim_scenario sce
		ON fact_19.scenario_id = sce.scenario_id
	WHERE sub_category = 'Telco Card' AND status_id = 1
)
, retained_table AS (
	SELECT acquisition_month, subsequent_month
		, COUNT(DISTINCT customer_id) retained_users
	FROM subsequent_table
	GROUP BY acquisition_month, subsequent_month
)
, source_table AS (
	SELECT *
    , FIRST_VALUE(retained_users) OVER (PARTITION BY acquisition_month ORDER BY acquisition_month ASC, subsequent_month ASC) original_users
    , FORMAT(retained_users*1.0/FIRST_VALUE(retained_users) OVER (PARTITION BY acquisition_month ORDER BY acquisition_month ASC, subsequent_month ASC),'P') as pct_retained
FROM retained_table
)
SELECT DISTINCT acquisition_month, original_users
	,[0],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11]
FROM (SELECT acquisition_month, subsequent_month, original_users, pct_retained FROM source_table) month_table

PIVOT (MAX(pct_retained) 
FOR subsequent_month IN([0],[1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11])) pivot_table
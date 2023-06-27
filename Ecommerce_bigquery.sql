-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month                                              

SELECT 
      FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) as month, 
      SUM(totals.visits) as visits, 
      SUM(totals.pageviews) as pageviews, 
      SUM(totals.transactions) as transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0101' AND '0331'
GROUP BY 1
ORDER BY 1;


-- Query 02: Bounce rate per traffic source in July 2017

-- Bounce session is the session that user does not raise any click after landing on the website
-- Bounce_rate = num_bounce/total_visit                     

WITH get_bounce_info_July2017 AS (
       SELECT 
             trafficSource.source AS source, 
             COUNT (trafficSource.source) AS total_visits, 
             SUM (totals.bounces) AS total_no_of_bounces,
       FROM  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
       GROUP BY 1)
 SELECT 
       source, 
       total_visits, 
       total_no_of_bounces, 
       total_no_of_bounces*100.0/total_visits AS bounce_rate
 FROM get_bounce_info_July2017
 ORDER BY 2 DESC;

--e có thể ghi trực tiếp luôn 
SELECT
    trafficSource.source as source,
    sum(totals.visits) as total_visits,
    sum(totals.Bounces) as total_no_of_bounces,
    (sum(totals.Bounces)/sum(totals.visits))* 100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC;


-- Query 3: Revenue by traffic source by week, by month in June 2017

-- separate month and week data then union all

SELECT 
      'Month' AS time_type,
      FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) as time,
      trafficSource.source AS source,
      SUM(product.productRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
, UNNEST (hits) hits
    , UNNEST (hits.product) product
    WHERE product.productRevenue IS NOT NULL

GROUP BY 2,3

UNION ALL

SELECT 
      'Week' AS time_type,
      FORMAT_DATE('%Y%W',PARSE_DATE('%Y%m%d', date)) AS time, 
      trafficSource.source AS source,
      SUM(product.productRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
, UNNEST (hits) hits
    , UNNEST (hits.product) product
    WHERE product.productRevenue IS NOT NULL
GROUP BY 2,3 
ORDER BY 4 DESC;


--Query 04: Average number of product pageviews by purchaser type in June, July 2017

-- Avg pageview = total pageview / number unique user
-- purchaser type: purchasers vs non-purchasers
-- totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser

WITH purchaser_data AS(
  SELECT
      FORMAT_DATE("%Y%m",parse_date("%Y%m%d",date)) AS month,
      (SUM(totals.pageviews)/COUNT(DISTINCT fullvisitorid)) AS avg_pageviews_purchase,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    , UNNEST(hits) hits
    , UNNEST(product) product
  WHERE _table_suffix BETWEEN '0601' AND '0731'
  AND totals.transactions>=1
  AND totals.totalTransactionRevenue IS NOT NULL
  AND product.productRevenue IS NOT NULL
  GROUP BY 1
),

non_purchaser_data AS(
  SELECT
      FORMAT_DATE("%Y%m",parse_date("%Y%m%d",date)) AS month,
      SUM(totals.pageviews)/COUNT(DISTINCT fullvisitorid) AS avg_pageviews_non_purchase,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      , UNNEST (hits) hits
    , UNNEST(product) product
  WHERE _table_suffix BETWEEN '0601' AND '0731'
  AND totals.transactions IS NULL
  AND product.productRevenue IS NULL
  GROUP BY 1
)

SELECT
    pd.*,
    avg_pageviews_non_purchase
FROM purchaser_data AS pd
LEFT JOIN non_purchaser_data 
USING(month)
ORDER BY pd.month;


-- Query 05: Average number of transactions per user that made a purchase in July 2017
  
SELECT
          FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) as month,
          SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId) AS avg_total_transactions_per_user
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST (hits) hits
    , UNNEST (hits.product) product
    WHERE product.productRevenue IS NOT NULL
    and totals.transactions >=1
    GROUP BY 1;



-- Query 6: Average amount of money spent per session

SELECT
      FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) AS month,
      ROUND((SUM(product.productRevenue)/1000000) / SUM(totals.visits),2) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST (hits) hits
, UNNEST (hits.product) product
WHERE product.productRevenue IS NOT NULL
AND totals.transactions IS NOT NULL
GROUP BY 1;



-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017

SELECT
    product.v2productname as other_purchased_product,
    sum(product.productQuantity) as quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) AS product
WHERE fullvisitorid IN (SELECT distinct fullvisitorid
                        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
                        , UNNEST(hits) AS hits
                        , UNNEST(hits.product) AS product
                        WHERE product.v2productname = "YouTube Men's Vintage Henley"
                        AND product.productRevenue IS NOT NULL)
AND product.v2productname != "YouTube Men's Vintage Henley"
AND product.productRevenue IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month


-- hits.eCommerceAction.action_type = '2' is view product page
-- hits.eCommerceAction.action_type = '3' is add to cart
-- hits.eCommerceAction.action_type = '6' is purchase


    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) AS month,
        COUNT(product.v2ProductName) AS num_product_view
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    , UNNEST (hits) AS hits
    , UNNEST (product) AS product
    WHERE _table_suffix BETWEEN '0101' AND '0331'
    AND eCommerceAction.action_type = '2'
    GROUP BY 1)

, addtocart AS (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) AS month,
        COUNT(product.v2ProductName) as num_addtocart
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    , UNNEST (hits) AS hits
    , UNNEST (product) AS product
    WHERE _table_suffix BETWEEN '0101' AND '0331'
    AND eCommerceAction.action_type = '3'
    GROUP BY 1)

, purchase AS (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) AS month,
        COUNT(product.v2ProductName) as num_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    , UNNEST (hits) AS hits
    , UNNEST (product) AS product
    WHERE _table_suffix BETWEEN '0101' AND '0331'
    AND eCommerceAction.action_type = '6'
    AND product.productRevenue IS NOT NULL
    GROUP BY 1)

SELECT
       month,
       product_view.num_product_view,
       addtocart.num_addtocart,
       purchase.num_purchase,
       ROUND((num_addtocart*100.0/num_product_view),2) as add_to_cart_rate,
       ROUND((num_purchase*100.0/num_product_view),2) as purchase_rate
FROM product_view
JOIN addtocart USING(month)
JOIN purchase USING(month)
ORDER BY 1;
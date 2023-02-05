

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT format_date('%Y%m',parse_date('%Y%m%d',date)) month,
      SUM(totals.visits) AS visits,
      SUM(totals.pageviews) AS pageviews,
      SUM(totals.transactions) AS transactions,
      SUM(totals.totalTransactionRevenue) /1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
group by month 
order by month


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
      trafficSource.source source,
      sum(totals.bounces)/sum(totals.visits) *100 as bounce_rate,
      sum(totals.visits)total_visits,
      sum (totals.bounces) total_no_of_bounces,
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170701' and '20170731'
group by source
order by total_visits desc


-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
select trafficSource.source source,    --lấy data của tháng
      'Month' time_type,
      format_date("%Y%m",parse_date('%Y%m%d',date)) time,
      sum(totals.totalTransactionRevenue)/1000000 revenue,
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170601' and '20170631'
group by source,time

UNION ALL

select trafficSource.source source,    --lấy data của tuần
      'Week' time_type,
      format_date("%Y%W",parse_date('%Y%m%d',date)) time,
      sum(totals.totalTransactionRevenue)/1000000 revenue,
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170601' and '20170631'
group by source,time
order by time_type,source ASC

with month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
)

select * from month_data
union all
select * from week_data


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with purchase as    --cte1 -> purchase
    (select	
          format_date('%Y%m',parse_date('%Y%m%d',date)) month,
          sum(totals.pageviews)/count(distinct fullVisitorId) avg_pageviews_purchase
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    where _table_suffix between '20170601' and '20170731'
    and totals.transactions>=1
    group by month
    order by avg_pageviews_purchase  

,non_purchase as   --cte2 -> non_purchase
    (select
          format_date('%Y%m',parse_date('%Y%m%d',date)) month,
          sum(totals.pageviews)/COUNT(distinct fullVisitorId) avg_pageviews_non_purchase
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    where _table_suffix between '20170601' and '20170731'
    and totals.transactions is null
    group by month
    order by avg_pageviews_non_purchase)

select purchase.month, purchase.avg_pageviews_purchase,non_purchase.avg_pageviews_non_purchase
from purchase p
LEFT join non_purchase np
on p.month = np.month  --using(month)



-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
select 
  format_date('%Y%m',parse_date('%Y%m%d',date)) Month,
  sum(totals.transactions)/ count(distinct fullVisitorId) avg_total_transations_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170701' and '20170731'
and totals.transactions >=1
group by month


-- Query 06: Average amount of money spent per session
#standardSQL
select 
    format_date('%Y%m',parse_date('%Y%m%d',date)) Month,
    sum(totals.totalTransactionRevenue)/ sum(totals.visits) avg_revenue_per_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170701' and '20170731'
and totals.transactions >=1
group by month



-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

-- CTE 1st method:
with product as(
        select
        fullVisitorId,
        product.v2ProductName,
        product.productQuantity,
        product.productRevenue 
        from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        unnest(hits) hits,unnest(hits.product) product
        where _table_suffix between '20170701' and '20170731'
        and product.productRevenue is not null)

select 
    product.v2ProductName other_purchased_products,
    sum(product.productQuantity) quantity
from product
where product.fullVisitorId in (select fullVisitorId from product
                                where product.v2ProductName = "YouTube Men's Vintage Henley")
and product.v2ProductName <> "YouTube Men's Vintage Henley"
group by other_purchased_products
order by quantity desc



--subquery method:
select
    product.v2productname as other_purchased_product,
    sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
where fullvisitorid in (select distinct fullvisitorid
                        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                        unnest(hits) as hits,
                        unnest(hits.product) as product
                        where product.v2productname = "YouTube Men's Vintage Henley"
                        and hits.eCommerceAction.action_type = '6')
and product.v2productname != "YouTube Men's Vintage Henley"
and product.productRevenue is not null
group by other_purchased_product
order by quantity desc

--CTE 2nd method:

with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
with a as(   --a -> view
  select 
  format_date('%Y%m',parse_date('%Y%m%d',date)) month,
  count(product.v2ProductName) num_product_view
  from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  unnest(hits) hits,
  unnest (hits.product) product
  where _table_suffix between '20170101' and '20170331'
  and eCommerceAction.action_type = "2"
  group by month	)
  , b as (   --b -> addtocart
select 
  format_date('%Y%m',parse_date('%Y%m%d',date)) month,
  count(product.v2ProductName) num_addtocart
  from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  unnest(hits) hits,
  unnest (hits.product) product
  where _table_suffix between '20170101' and '20170331'
  and eCommerceAction.action_type = "3"
  group by month)
  ,c as (   --c -> purchase
    select 
  format_date('%Y%m',parse_date('%Y%m%d',date)) month,
  count(product.v2ProductName) num_purchase
  from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  unnest(hits) hits,
  unnest (hits.product) product
  where _table_suffix between '20170101' and '20170331'
  and eCommerceAction.action_type = "6"
  and product.productRevenue is not null  --phải thêm đk này để lọc trùng data
  group by month
  )

  select 
        a.month,
        a.num_product_view,
        b.num_addtocart,
        c.num_purchase,
        (num_addtocart/num_product_view)*100 add_to_cart_rate,
        (num_purchase/num_product_view)*100 purchase_rate
  from a
  join b using(month)
  join c using(month)
order by month 


-- dùng count(case when) hoặc sum(case when)

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data


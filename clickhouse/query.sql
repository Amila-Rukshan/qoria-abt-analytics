-- Country-Level Revenue Table
SELECT
    country,
    sum(total_price) AS total_revenue,
    count() AS number_of_transactions
FROM
    transactions
GROUP BY
    country
ORDER BY
    total_revenue DESC;

-- Top 20 Frequently Purchased Products
SELECT
    product_name,
    sum(quantity) AS total_quantity_purchased,
    anyLast(stock_quantity) AS current_stock_quantity
FROM
    transactions
GROUP BY
    product_name
ORDER BY
    total_quantity_purchased DESC
LIMIT
    20;

-- Months with the highest sales volume
SELECT
    formatDateTime(transaction_date, '%M') AS month_name,
    sum(total_price) AS total_revenue
FROM
    transactions
GROUP BY
    month_name
ORDER BY
    total_revenue DESC;

-- Top 30 Regions by Revenue
SELECT
    region,
    sum(total_price) AS total_revenue,
    sum(quantity) AS total_items_sold
FROM
    transactions
GROUP BY
    region
ORDER BY
    total_revenue DESC
LIMIT
    30;

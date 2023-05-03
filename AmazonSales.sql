--Source: https://www.kaggle.com/datasets/georgejnr/used-and-new-cars-datasets


--Excel: Convert text to date in order_date column and change format to mm/dd/yyyy, otherwise values come in as NULL when importing dataset.


--Excel: Change hyphens to periods in order_id column, otherwise values come in as NULL when importing dataset.


-- Rename table to 'AmazonSales'


--Delete duplicates. Every row in the dataset has a unique item_id, even if SKUs in separate rows are matching, so this was excluded in partition.
--We are assuming that in a single order (where the order_id is the same), if the variables in the partition are all the same, then this is a duplicate. 

with DuplicateCheck as (SELECT *,
Row_Number() OVER (
Partition by [order_id], [order_date], [status], [sku], [qty_ordered], [price], [value], [discount_amount], [total], [category], [payment_method], [cust_id],
[bi_st], [cust_id], [year], [month], [ref_num], [full_name], [discount_percent]
ORDER BY [cust_id]) row_number
FROM AmazonSales
)
DELETE
FROM DuplicateCheck 
Where Row_number>1

--Standardize Date format

Alter Table AmazonSales
Add [OrderDate] date

Update AmazonSales
SET [OrderDate] = convert(Date, [order_date])

Alter Table AmazonSales
DROP COLUMN [order_date]


--Standardize [Sign in date] format

Alter Table AmazonSales
Add [Customer Signup] date

Update AmazonSales
SET [Customer Signup] = convert(Date, [sign in date])

SELECT * FROM AmazonSales

Alter Table AmazonSales
DROP COLUMN [Sign in date]

--Remove unnecessary columns

Alter Table AmazonSales
DROP Column [bi_st], [year], [month], [Name Prefix], [Middle Initial], [Last Name], [First Name], [E Mail], 
[Phone No#], [Place Name], [County], [City], [Zip], [user Name], [ref_num], [full_name]


--[qty_ordered] column values seem 1 higher than they should be. Check to see.

with qtycheck as (SELECT cast(([qty_ordered]-1)*[price] as decimal(10,2)) [valuecheck], cast([value] as decimal(10,2)) [valuecheck2]
FROM Amazonsales
),
matching as (SELECT [valuecheck], [valuecheck2],
CASE WHEN [valuecheck] = [valuecheck2] THEN 0
ELSE 1 END match
FROM qtycheck qc
)
SELECT [valuecheck], [valuecheck2], [match]
FROM matching m
WHERE [match] = 1


--Create new column [qty]: Subtract 1 from qty_ordered.

Alter Table AmazonSales
Add [qty] int

Update AmazonSales
SET [qty] = ([qty_ordered]-1)

Alter Table AmazonSales
DROP Column [qty_ordered]


--Round discount_amount, total, and discount_percent columns to only two decimal places

Update AmazonSales
SET [discount_amount] = convert(decimal(10,2), [discount_amount])

Update AmazonSales
SET [total] = convert(decimal(10,2), [total])

Update AmazonSales
SET [discount_percent] = convert(decimal(10,2), [discount_percent])


--Create two new columns [OrderDate_Month] and [OrderDate_Year]

Alter Table AmazonSales
Add [OrderDate_Month] int

Update AmazonSales
SET [OrderDate_Month] = month([OrderDate])

Alter Table AmazonSales
Add [OrderDate_Year] int

Update AmazonSales
SET [OrderDate_Year] = year([OrderDate])


--Create new column: status-simplified

Alter Table AmazonSales
Add [status-simplified] varchar(50)

Update AmazonSales
SET [status-simplified] = CASE 
WHEN status = 'canceled' then 'Incomplete'
WHEN status = 'complete' then 'Complete'
WHEN status = 'received' then 'Complete'
WHEN status = 'order_refunded' then 'Incomplete'
WHEN status = 'refund' then 'Incomplete'
WHEN status = 'cod' then 'Complete'
WHEN status = 'paid' then 'Complete'
WHEN status = 'pending' then 'Incomplete'
WHEN status = 'closed' then 'Incomplete'
WHEN status = 'holded' then 'Incomplete'
WHEN status = 'processing' then 'Incomplete'
WHEN status = 'payment_review' then 'Incomplete'
WHEN status = 'pending_paypal' then 'Incomplete'
END


--View revised dataset

SELECT * FROM AmazonSales
WHERE [order_id] = '100546199.1'


--Create view for customers who are currently inactive:

Create view [Inactive Customers] 
AS
with [Complete Items] as (SELECT [cust_id], [order_id], [OrderDate], [discount_amount], [value], 
Max(OrderDate) OVER (partition by cust_id) [LastOrderDate]
FROM AmazonSales
where [status-simplified] = 'complete'
)
,
[Inactive Customer Orders] as 
(
SELECT [cust_id], [order_id], [OrderDate], [LastOrderDate], sum([discount_amount]) [Order Discounts], sum([value]) [Order Sales],
sum([Discount_amount])/(CASE WHEN sum([Value]) = 0 THEN NULL ELSE sum([value]) END) AS [Discount-to-Sales by order],
percentile_cont(0.5) within group (Order by sum([Discount_amount])/(CASE WHEN sum([Value]) = 0 THEN NULL ELSE sum([value]) END)) OVER (Partition by [cust_id]) 
AS [Discount-to-Sales of Orders Median]
FROM [Complete Items]
WHERE datediff(day,[LastOrderDate],'2021-09-30') > 180
GROUP BY [cust_id], [order_id], [OrderDate], [LastOrderDate]
)
,
[Ratios] AS 
(
SELECT [cust_id], [order_id], [orderdate], [LastOrderDate], [Order Discounts], [Order Sales], [Discount-to-Sales by Order],
avg([Order Sales]) OVER (Partition by [cust_id]) [Expected Sale of Next Order],
AVG([Discount-to-Sales by Order]) over (Partition by [cust_id]) [Discount-to-Sales of Orders Weighted],
[Discount-to-Sales of Orders Median]
FROM [Inactive Customer Orders]
GROUP BY [Cust_id], [order_id], [orderdate], [LastOrderDate], [Order Discounts], [Order Sales], [Discount-to-Sales by Order], [Discount-to-Sales of Orders Median]
)
,
[Customers] AS
(
SELECT [cust_id], [LastOrderDate], sum([Order Discounts]) [Customer Discounts], sum([Order Sales]) [Customer Sales], 
Cast ([Expected Sale of Next Order] AS Decimal(10,2)) [Expected Sale of Next Order],
sum(CASE WHEN [Order Discounts] > 0 THEN 1 ELSE 0 END) [Orders with Discounts], count([Order_id]) [Number of Orders],
Cast (sum([Order Discounts])/(Case WHEN sum([Order Sales]) = '0' THEN NULL ELSE sum([Order Sales]) END) AS decimal(10,4)) [Discount-to-Sales of Orders Aggregated],
Cast (avg([Discount-to-Sales of Orders Weighted]) AS decimal(10,4)) [Discount-to-Sales of Orders Weighted], 
Cast (avg([Discount-to-Sales of Orders Median]) AS decimal(10,4)) [Discount-to-Sales of Orders Median]
FROM [Ratios]
GROUP BY [cust_id], [LastOrderDate], [Expected Sale of Next Order]
HAVING sum([Order Sales]) > 0
)
,
[Discount Options] as 
(
SELECT [cust_id], [LastOrderDate], [Customer Discounts], [Customer Sales], [Expected Sale of Next Order], 
[Orders with Discounts], [Number of Orders],
Cast ([Orders with Discounts] / [Number of Orders] AS Decimal (10,4)) [Share of Orders with Discounts],
[Discount-to-Sales of Orders Aggregated], [Discount-to-Sales of Orders Weighted], [Discount-to-Sales of Orders Median],
greatest([Discount-to-Sales of Orders Aggregated], [Discount-to-Sales of Orders Weighted], [Discount-to-Sales of Orders Median]) AS [Discounts - Passive],
greatest([Discount-to-Sales of Orders Aggregated], [Discount-to-Sales of Orders Weighted], [Discount-to-Sales of Orders Median]) + .05 AS [Discounts - Slight Increase],
greatest([Discount-to-Sales of Orders Aggregated], [Discount-to-Sales of Orders Weighted], [Discount-to-Sales of Orders Median]) + .1 AS [Discounts - Moderate Increase],
greatest([Discount-to-Sales of Orders Aggregated], [Discount-to-Sales of Orders Weighted], [Discount-to-Sales of Orders Median]) + .15 AS [Discounts - Aggressive Increase]
FROM [Customers]
)
,
[Next Order Discounts] as
(
SELECT [cust_id], [LastOrderDate], [Customer Discounts], [Customer Sales], percent_rank() OVER (Order by [Customer Sales]) [Sales Rank], [Expected Sale of Next Order],
[Orders with Discounts], [Number of Orders], 
[Discount-to-Sales of Orders Aggregated], [Discount-to-Sales of Orders Weighted], [Discount-to-Sales of Orders Median],
[Discounts - Passive], 1 + [Discounts - Passive] [Discount Rank], 
Cast ([Discounts - Passive] * [Expected Sale of Next Order] AS Decimal(10,2)) [Passive Discount Expected on Next Order],
[Discounts - Slight Increase], CAST ([Discounts - Slight Increase]*[Expected Sale of Next Order] AS Decimal(10,2)) [Slight Increase Discount Expected on Next Order],
[Discounts - Moderate Increase], Cast ([Discounts - Moderate Increase]*[Expected Sale of Next Order] AS Decimal (10,2)) [Moderate Increase Discount Expected on Next Order],
[Discounts - Aggressive Increase], Cast ([Discounts - Aggressive Increase]*[Expected Sale of Next Order] AS Decimal (10,2)) [Aggressive Increase Discount Expected on Next Order]
FROM [Discount Options]
)
SELECT [cust_id], [LastOrderDate], [Customer Discounts], [Customer Sales], cast (100*[Sales Rank]/[Discount Rank] AS DECIMAL(10,4)) [Discount Benefit Score], 
[Expected Sale of Next Order], [Orders with Discounts], [Number of Orders],
[Discount-to-Sales of Orders Aggregated], [Discount-to-Sales of Orders Weighted], [Discount-to-Sales of Orders Median], 
[Discounts - Passive], [Passive Discount Expected on Next Order],
[Discounts - Slight Increase], [Slight Increase Discount Expected on Next Order],
[Discounts - Moderate Increase], [Moderate Increase Discount Expected on Next Order],
[Discounts - Aggressive Increase], [Aggressive Increase Discount Expected on Next Order]
FROM [Next Order Discounts]


-- Comprehensive view of purchases for inactive customers

Create view [Comprehensive Orders of Inactive Customers]
AS 
Select a.[cust_id], [item_id], [order_id], [OrderDate], [price], [qty], [value], [discount_amount], [total], [sku], [category], [Gender], [Age], [State], 
[Region], [payment_method], [customer signup],
[Discount Benefit Score], [Discounts - Passive], [Expected Sale of Next Order]
FROM AmazonSales a
JOIN [Inactive Customers] c
ON a.[Cust_id] = c.[Cust_id]
WHERE [status-simplified] = 'complete'


-- Create view for customers who were previously inactive: 

Create View [Previous Inactive Customers]
AS
with [LagOrders] as (SELECT [cust_id], [order_id], [OrderDate], 
lag([OrderDate]) Over (Partition by Cust_id Order by [OrderDate] ASC, [Order_id] ASC) [LagOrderDate],
sum([discount_amount])[Order Discounts], sum([value]) [Order Sales],
Cast (sum([Discount_amount])/(Case WHEN sum([value]) = '0' THEN NULL ELSE sum([value]) END) AS decimal(10,4)) [Discount-to-Sales by Order]
FROM AmazonSales
where [status-simplified] = 'complete'
GROUP BY [order_id], [cust_id], [OrderDate]
)
,
[LateOrders] as
(
SELECT [cust_id], [order_id] AS [Late Order_id], [OrderDate] AS [Late OrderDate], [LagOrderDate] AS [Late LagOrderDate], [Order Discounts] AS [Late OrderDiscount], 
[Order Sales] AS [Late OrderSale], [Discount-to-Sales by Order] [Discount-to-Sales of LateOrder]
FROM [LagOrders]
WHERE Datediff(day,[LagOrderDate],[OrderDate]) > '180' AND [Discount-to-Sales by Order] is not null 
)
,
[PreviousOrders] as
(
select L2.[cust_id], L1.[order_id], L1.[orderdate], L1.[Order Discounts], L1.[Order Sales], L1.[Discount-to-sales by Order], 
sum(L1.[Order Discounts]) OVER (Partition by L1.cust_id) [TotalDiscounts of PreviousOrders], sum(L1.[Order Sales]) OVER (Partition by L1.cust_id) [TotalSales of PreviousOrders],
[Late OrderDate], [Late LagOrderDate], [Late Order_id], [Late OrderDiscount], [Late OrderSale], [Discount-to-Sales of LateOrder],
percentile_cont(0.5) within group (Order by L1.[Discount-to-sales by Order]) OVER (Partition by l2.cust_id) AS [Discount-to-Sales of PreviousOrdersMedian]
FROM [Lagorders] L1
JOIN [LateOrders] L2 ON L1.Cust_id=L2.cust_id
WHERE L1.OrderDate < L2.[Late OrderDate]
)
,
[Previous Order Ratios] as 
(
SELECT cust_id, [Late OrderDate], [Late Order_id], [Late OrderDiscount], [Late OrderSale], [TotalSales of PreviousOrders],
Sum([TotalDiscounts of PreviousOrders]) / (CASE WHEN Sum([TotalSales of PreviousOrders]) = 0 
THEN NULL ELSE Sum([TotalSales of PreviousOrders]) END) [Discount-to-Sales of PreviousOrdersAggregated],
avg([Discount-to-sales by Order]) [Discount-to-Sales of PreviousOrdersWeighted],
avg([Discount-to-Sales of PreviousOrdersMedian]) [Discount-to-Sales of PreviousOrdersMedian]
FROM [PreviousOrders] 
GROUP BY cust_id, [Late OrderDate], [Late Order_id], [Late OrderDiscount], [Late OrderSale], [TotalSales of PreviousOrders]
)
,
[Discount Options] AS
(
SELECT r.cust_id, r.[Late OrderDate], r.[Late Order_id], r.[Late OrderDiscount], r.[Late OrderSale], r.[TotalSales of PreviousOrders],
Cast (r.[Discount-to-Sales of PreviousOrdersAggregated] AS decimal(10,4)) [Discount-to-Sales of PreviousOrdersAggregated],
Cast (r.[Discount-to-Sales of PreviousOrdersWeighted] AS decimal(10,4)) [Discount-to-Sales of PreviousOrdersWeighted],
Cast (r.[Discount-to-Sales of PreviousOrdersMedian] AS decimal(10,4)) [Discount-to-Sales of PreviousOrdersMedian],
Cast (L2.[Discount-to-Sales of LateOrder] AS decimal(10,4)) [Discount-to-Sales of LateOrder], 
Cast (greatest([Discount-to-Sales of PreviousOrdersAggregated], [Discount-to-Sales of PreviousOrdersWeighted], 
[Discount-to-Sales of PreviousOrdersMedian]) AS Decimal(10,4)) [Discounts - Passive],
Cast (greatest([Discount-to-Sales of PreviousOrdersAggregated], [Discount-to-Sales of PreviousOrdersWeighted], 
[Discount-to-Sales of PreviousOrdersMedian]) + 0.05 AS Decimal(10,4)) [Discounts - Slight Increase],
Cast (greatest([Discount-to-Sales of PreviousOrdersAggregated], [Discount-to-Sales of PreviousOrdersWeighted], 
[Discount-to-Sales of PreviousOrdersMedian]) + 0.1 AS Decimal(10,4)) [Discounts - Moderate Increase],
Cast (greatest([Discount-to-Sales of PreviousOrdersAggregated], [Discount-to-Sales of PreviousOrdersWeighted], 
[Discount-to-Sales of PreviousOrdersMedian]) + 0.15 AS Decimal(10,4)) [Discounts - Aggressive Increase]
FROM [Previous Order Ratios] r
JOIN [LateOrders] L2 ON r.cust_id=L2.cust_id
WHERE [Discount-to-Sales of PreviousOrdersAggregated] is NOT NULL AND [Discount-to-Sales of PreviousOrdersWeighted] is not NULL
AND [Discount-to-Sales of PreviousOrdersMedian] is NOT NULL
)
SELECT cust_id, [Late OrderDate], [Late Order_id], [Late OrderDiscount], [Late OrderSale], [TotalSales of PreviousOrders],
[Discount-to-Sales of PreviousOrdersAggregated], [Discount-to-Sales of PreviousOrdersWeighted], [Discount-to-Sales of PreviousOrdersMedian], [Discount-to-Sales of LateOrder],
[Discounts - Passive],
Cast (CASE WHEN [Discount-to-Sales of LateOrder] > [Discounts - Passive] THEN [Discount-to-Sales of LateOrder]*[Late OrderSale] ELSE 
[Discounts - Passive]*[Late OrderSale] END AS Decimal(10,2)) [Passive Discount on Late Order],
[Discounts - Slight Increase],
Cast (CASE WHEN [Discount-to-Sales of LateOrder] > [Discounts - Slight Increase] THEN [Discount-to-Sales of LateOrder]*[Late OrderSale] ELSE 
[Discounts - Slight Increase]*[Late OrderSale] END AS Decimal(10,2)) [Slight Increase Discount on Late Order],
[Discounts - Moderate Increase],
Cast (CASE WHEN [Discount-to-Sales of LateOrder] > [Discounts - Moderate Increase] THEN [Discount-to-Sales of LateOrder]*[Late OrderSale] ELSE 
[Discounts - Moderate Increase]*[Late OrderSale] END AS Decimal(10,2)) [Moderate Increase Discount on Late Order],
[Discounts - Aggressive Increase],
Cast (CASE WHEN [Discount-to-Sales of LateOrder] > [Discounts - Aggressive Increase] THEN [Discount-to-Sales of LateOrder]*[Late OrderSale] ELSE 
[Discounts - Aggressive Increase]*[Late OrderSale] END AS Decimal(10,2)) [Aggressive Increase Discount on Late Order],
Case WHEN [Discount-to-Sales of LateOrder] > Greatest([Discount-to-Sales of PreviousOrdersAggregated], [Discount-to-Sales of PreviousOrdersWeighted], 
[Discount-to-Sales of PreviousOrdersMedian]) THEN 'Utilized' ELSE 'Potential' END [Discount Strategy]
FROM [Discount Options]
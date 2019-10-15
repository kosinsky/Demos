-- Create test table with clustered column store index
CREATE TABLE [dbo].[FactOnlineSales_Wide](
	[OnlineSalesKey] [int]  NOT NULL,
	[DateKey] [datetime] NOT NULL,
	[StoreKey] [int] NOT NULL,
	[ProductKey] [int] NOT NULL,
	[PromotionKey] [int] NOT NULL,
	[CurrencyKey] [int] NOT NULL,
	[CustomerKey] [int] NOT NULL,
	[SalesOrderNumber] [nvarchar](20) NOT NULL,
	[SalesOrderLineNumber] [int] NULL,
	[SalesQuantity] [int] NOT NULL,
	[SalesAmount] [money] NOT NULL,
	[ReturnQuantity] [int] NOT NULL,
	[ReturnAmount] [money] NULL,
	[DiscountQuantity] [int] NULL,
	[DiscountAmount] [money] NULL,
	[TotalCost] [money] NOT NULL,
	[UnitCost] [money] NULL,
	[UnitPrice] [money] NULL,
	[ETLLoadID] [int] NULL,
	[LoadDate] [datetime] NULL,
	[UpdateDate] [datetime] NULL
 )
 GO

 CREATE CLUSTERED COLUMNSTORE INDEX CI_FactOnlineSales_Wide ON FactOnlineSales_Wide
 GO

-- We have 20 columns now

-- Inject 1M records in batches smaller than 100*1024 to write to delta stora
-- 1M records is less than 1024*1024 => compression will not happen
INSERT INTO [FactOnlineSales_Wide]
(
	OnlineSalesKey,
	DateKey,
	StoreKey,
	ProductKey,
	PromotionKey,
	CurrencyKey,
	CustomerKey,
	SalesOrderNumber,
	SalesOrderLineNumber,
	SalesQuantity,
	SalesAmount,
	ReturnQuantity,
	ReturnAmount,
	DiscountQuantity,
	DiscountAmount,
	TotalCost,
	UnitCost,
	UnitPrice,
	ETLLoadID,
	LoadDate,
	UpdateDate
)

SELECT TOP(20000) 
	OnlineSalesKey,
	DateKey,
	StoreKey,
	ProductKey,
	PromotionKey,
	CurrencyKey,
	CustomerKey,
	SalesOrderNumber,
	SalesOrderLineNumber,
	SalesQuantity,
	SalesAmount,
	ReturnQuantity,
	ReturnAmount,
	DiscountQuantity,
	DiscountAmount,
	TotalCost,
	UnitCost,
	UnitPrice,
	ETLLoadID,
	LoadDate,
	UpdateDate
FROM [FactOnlineSales]
GO 50 -- SSMS will repeat script above 50 times

-- Meassure delta sote size
SELECT *, size_in_bytes/1024/1024 as [SizeMb]
FROM sys.column_store_row_groups 
WHERE object_id=OBJECT_ID('[dbo].[FactOnlineSales_WIDE]')

-- It's 156Mb => every single query will read it 


-- 3. Show that Update in delta store is insert + delete
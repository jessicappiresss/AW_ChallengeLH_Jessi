with
    SalesOrderPrincipalTable as (
        select *
        from {{ ref('Sales_Order_Detail') }}
    )

    , SelectingColumnsDimSalesOrder as (
        select
            PK_SalesOrderDetail
            , Fk_SalesOrder
            , Fk_Customer
            , Fk_Territory
            , Fk_BusinessPerson
            , SalesOrderId
            , ProductId
            , OrderProductQuantity
            , ProductUnitPrice
            , SpecialOfferId
            , ProductUnitPriceDiscount
            , TerritoryId
            , TerritoryName
            , CountryRegionCode
            , SalesTerritoryAccumulatedYear
            , TotalSalesYear
            , CommercialCostsTerritoryYear
            , CommercialCostsTerritoryPreviousYear
            , CreditCardId
            , CustomerId
            , ShipToAddressId
            , ShipMethodId
            , BillToAddressId
            , OrderDate
            , DueDate
            , ShipDate
            , OrderStatus
            , OnlineOrderType
            , OrderSubtotalValue
            , OrderTotalValue
            , BusinessPersonid
            , ProjectionAnnualSales
            , BonusWhenGoalAchieved
            , TotalPersonSalesYear
            , TotalSalesPreviousYear
            , SalesPersonId
            , StoreBusinessPersonId
            , StoreName
        from SalesOrderPrincipalTable
    )

    , CreatingSkProduct as (
        select
            farm_fingerprint(cast(PK_SalesOrderDetail as string)) as Sk_SalesOrderDetail
            , SelectingColumnsDimSalesOrder.*
        from SelectingColumnsDimSalesOrder
    )

select *
from CreatingSkProduct

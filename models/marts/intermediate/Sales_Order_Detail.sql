with
    SalesOrderDetailTable as (
        select *
        from {{ ref('stg_erp_Sales_Order_Detail') }}
    )

    , SalesOrderHeader as (
        select *
        from {{ ref('Sales_Order_Header') }}
    )

    , Customer as (
        select *
        from {{ ref('Customer') }}
    )

    , SalesTerritory as (
        select *
        from {{ ref('Sales_Territory') }}
    )

    , SalesPerson as (
        select *
        from {{ ref('Sales_Person') }}
    )

    , Store as (
        select *
        from {{ ref('Store') }}
    )

    , FirstJoinSOD as (
        select 
            SalesOrderHeader.Pk_SalesOrder as Fk_SalesOrder
            , SalesOrderHeader.TerritoryId
            , SalesOrderHeader.CreditCardId
            , SalesOrderHeader.CustomerId
            , SalesOrderHeader.SalesPersonId
            , SalesOrderHeader.ShipToAddressId
            , SalesOrderHeader.ShipMethodId
            , SalesOrderHeader.BillToAddressId
            , SalesOrderHeader.OrderDate
            , SalesOrderHeader.DueDate
            , SalesOrderHeader.ShipDate
            , SalesOrderHeader.OrderStatus
            , SalesOrderHeader.OnlineOrderType
            , SalesOrderHeader.OrderSubtotalValue
            , SalesOrderHeader.OrderTotalValue
            , SalesOrderDetailTable.*
        from SalesOrderDetailTable
        inner join SalesOrderHeader
            on SalesOrderDetailTable.SalesOrderId = SalesOrderHeader.SalesOrderId
    )

    , SecondJoinSOD as (
        select
            Customer.Pk_Customer as Fk_Customer
            , Customer.PersonId
            , Customer.StoreId
            , FirstJoinSOD.*
        from FirstJoinSOD
        left join Customer
            on FirstJoinSOD.CustomerId = Customer.CustomerId
    )

    , ThirdJoinSOD as (
        select
            SalesTerritory.Pk_Territory as Fk_Territory
            , SalesTerritory.TerritoryName
            , SalesTerritory.CountryRegionCode
            , SalesTerritory.SalesTerritoryAccumulatedYear
            , SalesTerritory.TotalSalesYear
            , SalesTerritory.CommercialCostsTerritoryYear
            , SalesTerritory.CommercialCostsTerritoryPreviousYear
            , SecondJoinSOD.*
        from SecondJoinSOD
        left join SalesTerritory
            on SecondJoinSOD.TerritoryId = SalesTerritory.TerritoryId
    )

    , FourthJoinSOD as (
        select
            SalesPerson.Pk_BusinessPerson as Fk_BusinessPerson
            , SalesPerson.BusinessPersonId
            , SalesPerson.ProjectionAnnualSales
            , SalesPerson.BonusWhenGoalAchieved
            , SalesPerson.TotalPersonSalesYear
            , SalesPerson.TotalSalesPreviousYear
            , ThirdJoinSOD.*
        from ThirdJoinSOD
        left join SalesPerson
            on ThirdJoinSOD.TerritoryId = SalesPerson.TerritoryId
    )

    , FinalJoinSOD as (
        select
            Store.StoreName
            , FourthJoinSOD.*
        from FourthJoinSOD
        left join Store
            on FourthJoinSOD.BusinessPersonId = Store.BusinessPersonId
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(SalesOrderDetailId  as string)) as Pk_SalesOrderDetail
            , FinalJoinSOD.*
        from FinalJoinSOD
    )

select *
from CreatingPk

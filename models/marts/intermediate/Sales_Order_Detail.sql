with
    SalesOrderDetailTable as (
        select *
        from {{ ref('stg_erp_Sales_Order_Detail') }}
    )

    , SalesOrderHeader as (
        select *
        from {{ ref('Sales_Order_Header') }}
    )

    , CustomerTable as (
        select *
        from {{ ref('Customer') }}
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
        left join SalesOrderHeader
            on SalesOrderDetailTable.SalesOrderId = SalesOrderHeader.SalesOrderId
    )

    , SecondJoin as (
        select
            CustomerTable.Pk_Customer
            , CustomerTable.StoreId
            , CustomerTable.PersonId
            , FirstJoinSOD.*
        from FirstJoinSOD
        left join CustomerTable
            on FirstJoinSOD.CustomerId = CustomerTable.CustomerId
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(SalesOrderDetailId  as string)) as Pk_SalesOrderDetail
            , SecondJoin.*
        from SecondJoin
    )

select *
from CreatingPk

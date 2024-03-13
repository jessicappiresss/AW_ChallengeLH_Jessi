with
    SalesOrderPrincipalTable as (
        select *
        from {{ ref('Sales_Order_Detail') }}
    )

    , SelectingColumnsDimSalesOrder as (
        select
            PK_SalesOrderDetail
            , Fk_SalesOrder
            , SalesOrderDetailId
            , SalesOrderId
            , ProductId
            , OrderProductQuantity
            , ProductUnitPrice
            , SpecialOfferId
            , ProductUnitPriceDiscount
            , TerritoryId
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
            , Pk_Customer as Fk_Customer
            , StoreId
            , SalesPersonId
            , PersonId
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

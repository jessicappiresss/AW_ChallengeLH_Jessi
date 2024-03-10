with
    SalesReasonTable as (
        select *
        from {{ ref('Sales_Reason') }}
    )

    , SalesOrderHeaderSalesReasonTable as (
        select *
        from {{ ref('Sales_Order_Header_Sales_Reason') }}
    )

    , JoinTable as (
        select
            SalesOrderHeaderSalesReasonTable.SalesOrderId
            , SalesOrderHeaderSalesReasonTable.Pk_SalesOrder
            , SalesReasonTable.*
        from SalesReasonTable
        left join SalesOrderHeaderSalesReasonTable
            on SalesReasonTable.SalesReasonId = SalesOrderHeaderSalesReasonTable.SalesReasonId
    )

    , NewSalesReasonTable as (
        select *
        from JoinTable
    )

    , SelectingSalesReasonTable as (
        select
            Pk_SalesReason
            , Pk_SalesOrder as Fk_SalesOrder
            , SalesReasonId
            , SalesOrderId
            , ReasonGivenName
            , ReasonType
        from NewSalesReasonTable
    )

    , CreatingSkSalesReason as (
        select
            farm_fingerprint(cast(Pk_SalesReason as string)) as Sk_SalesReason
            , SelectingSalesReasonTable.*
        from SelectingSalesReasonTable
    )

select *
from CreatingSkSalesReason

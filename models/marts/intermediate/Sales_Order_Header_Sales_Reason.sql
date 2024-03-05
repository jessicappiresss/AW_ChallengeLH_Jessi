with
    SalesHeaderReason as (
        select *
        from {{ ref('stg_erp_Sales_Order_Header_Sales_Reason') }}
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(SalesOrderId as string)) as Pk_SalesOrder
            , SalesOrderId
            , SalesReasonId
        from SalesHeaderReason
    )

select *
from CreatingPk

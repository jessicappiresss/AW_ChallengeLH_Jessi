with
    SalesOrderHeaderTable as (
        select *
        from {{ ref('stg_erp_Sales_Order_Header') }}
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(SalesOrderID  as string)) as Pk_SalesOrder
            , *
        from SalesOrderHeaderTable
    )

select *
from CreatingPk

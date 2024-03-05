with
    SalesOrderReasonStaging as (
        select
            cast(salesorderid as int) as SalesOrderId
            , cast(salesreasonid  as int) as SalesReasonId
        from {{ source('sales', 'salesorderheadersalesreason') }}
    )

select *
from SalesOrderReasonStaging

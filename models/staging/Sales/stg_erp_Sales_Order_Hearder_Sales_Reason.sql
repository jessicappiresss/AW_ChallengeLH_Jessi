with
    SalesOrderReasonStaging as (
        select
            cast(salesreasonid as int) as SalesReasonId
            , cast(salesreasonid  as int) as SalesReasonId
        from {{ source('sales', 'salesorderheadersalesreason') }}
    )

select *
from SalesOrderReasonStaging

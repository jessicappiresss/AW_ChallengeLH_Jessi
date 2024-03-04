with
    SalesOrderDetailStaging as (
        select
            cast(salesorderdetailid as int) as SalesOrderDetailId
            , cast(salesorderid as int) as SalesOrderId
            , cast(orderqty as int) as OrderProductQuantity
            , cast(productid as int) as ProductId
            , cast(specialofferid  as int) as SpecialOfferId
            , cast(unitprice as float) as ProductUnitPrice
            , cast(unitpricediscount as float) as ProductUnitPriceDiscount
        from {{ source('sales', 'salesorderdetail') }}
    )

select *
from SalesOrderDetailStaging

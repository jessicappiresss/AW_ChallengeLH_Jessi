with
    SalesOrderHeaderStaging as (
        select
            cast(salesorderid as int) as SalesOrderId
            , cast(territoryid as int) as TerritoryId
            , cast(creditcardid as int) as CreditCardId
            , cast(customerid as int) as CustomerId
            , cast(salespersonid as int) as SalesPersonId
            , cast(shiptoaddressid as int) as ShipToAddressId
            , cast(shipmethodid as int) as ShipMethodId
            , cast(billtoaddressid as int) as BilltoAddressId
            , cast(orderdate as timestamp) as OrderDate
            , cast(duedate as timestamp) as DueDate
            , cast(shipdate as timestamp) as ShipDate
            , cast(status as string) as Status
            , case
                when status = 1 then 'In Process'
                when status = 2 then 'Approved'
                when status = 3 then 'Order on hold'
                when status = 4 then 'Rejected'
                when status = 5 then 'Sent'
                when status = 6 then 'Canceled'
                else 'Unknown'
            end as OrderStatus
            , cast(onlineorderflag as string) OnlineOrderFlag
            , case
                when onlineorderflag = true then 'Order placed by the seller'
                when onlineorderflag = false then 'Order placed online by the customer'
                else 'Unknown'
            end as OnlineOrderType
            , cast(subtotal as float64) as OrderSubtotalValue
            , cast(totaldue as float64) as OrderTotalValue
        from {{ source('sales', 'salesorderheader') }}
    )

select *
from SalesOrderHeaderStaging

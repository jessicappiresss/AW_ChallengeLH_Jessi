with
    SalesOrder as (
        select *
        from {{ ref('Dim_Sales_Order') }}
    )

    , Products as (
        select *
        from {{ ref('Dim_Produtcs') }}
    )

    , SalesReason as (
        select *
        from {{ ref('Dim_Sales_Reason') }}
    )

    , AddressT as (
        select *
        from {{ ref('Dim_Address') }}
    )

    , Person as (
        select *
        from {{ ref('Dim_Person') }}
    )

    , CreditCard as (
        select *
        from {{ ref('Dim_Credit_Card') }}
    )

    , FactSalesFirst as (
        select
            Products.Sk_Products
            , Products.ProductCategoryId
            , Products.ProductSubcategoryId
            , Products.ProductName
            , Products.ProductCategoryName
            , Products.ProductSubCategoryName
            , Products.ManufactureTypes
            , Products.ProductMinimumStockQuantity
            , Products.ProductReorderPoint
            , Products.ProductStandardCost
            , Products.ProductSellPrice
            , Products.ManufactureProductInDays
            , SalesOrder.*
        from SalesOrder
        left join Products
            on SalesOrder.ProductId = Products.ProductId
    )

    , FactSalesSecond as (
        select
            SalesReason.Sk_SalesReason
            , SalesReason.SalesReasonId
            , SalesReason.ReasonGivenName
            , SalesReason.ReasonType
            , FactSalesFirst.*
        from FactSalesFirst
        left join SalesReason
            on FactSalesFirst.SalesOrderId = SalesReason.SalesOrderId
    )

    , FactSalesThird as (
        select
            AddressT.Sk_Territory
            , AddressT.StateProvinceAbbreviation
            , AddressT.RegionName
            , AddressT.CountryName
            , AddressT.PrincipalAddressId
            , AddressT.AddressRoadName
            , AddressT.AddressCityName
            , AddressT.PostalCode
            , FactSalesSecond.*
        from FactSalesSecond
        left join AddressT
            on FactSalesSecond.TerritoryId = AddressT.TerritoryId
    )

    , FactSalesFourth as (
        select
            Person.Sk_BusinessPerson
            , Person.PersonType
            , Person.PersonTypeDescription
            , Person.TitleName 
            , Person.CompleteName
            , Person.EmailAddressId
            , Person.EmailAddress
            , Person.AddressId
            , Person.AddressTypeId
            , FactSalesThird.*
        from FactSalesThird
        left join Person
            on FactSalesThird.BusinessPersonId = Person.BusinessPersonId
    )

    , FactSalesFinal as (
        select
            CreditCard.Sk_CreditCard
            , CreditCard.CardBrandType
            , FactSalesFourth.*
        from FactSalesFourth
        left join CreditCard
            on FactSalesFourth.CreditCardId = CreditCard.CreditCardId
    )

    , CreatingSkFact as (
        select
            farm_fingerprint(cast(Pk_SalesOrderDetail as string)) as Sk_FactSales
            , FactSalesFinal.*
        from FactSalesFinal
    )

select *
from CreatingSkFact

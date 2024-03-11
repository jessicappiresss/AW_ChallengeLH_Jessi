with
    SalesOrder as (
        select *
        from {{ ref('Dim_Sales_Order') }}
    )

    , Products as (
        select *
        from {{ ref('Dim_Produtcs') }}
    )
    
    , ReasonTypeTable as (
        select *
        from {{ ref('Dim_Sales_Reason') }}
    )

    , CreditCardTable as (
        select *
        from {{ ref('Dim_Credit_Card') }}
    )

    , PersonTable as (
        select *
        from {{ ref('Dim_Person') }}
    )

    , AddressTable as (
        select *
        from {{ ref('Dim_Address') }}
    )

    , FctTable as (
        select
            Products.Sk_Products
            , Products.ProductName
            , Products.ProductCategoryName
            , Products.ProductSellPrice
            , Products.ProductStandardCost
            , SalesOrder.*
        from SalesOrder
        left join Products
            on SalesOrder.ProductId = Products.ProductId
    )

    , FctTableJoin2 as (
        select
            ReasonTypeTable.Sk_SalesReason
            , ReasonTypeTable.SalesReasonId
            , ReasonTypeTable.ReasonGivenName
            , FctTable.*
        from FctTable
        left join ReasonTypeTable
            on FctTable.SalesOrderId = ReasonTypeTable.SalesOrderId
    )

    , FctTableJoin3 as (
        select
            CreditCardTable.BusinessPersonId
            , FctTableJoin2.*
        from FctTableJoin2
        left join CreditCardTable
            on FctTableJoin2.CreditCardId = CreditCardTable.CreditCardId
    )

    , FctTableJoin4 as (
        select
            PersonTable.PersonTypeDescription
            , PersonTable.CompleteName
            , PersonTable.EmailAddressId
            , PersonTable.EmailAddress
            , PersonTable.AddressID
            , FctTableJoin3.*
        from FctTableJoin3
        left join PersonTable
            on FctTableJoin3.BusinessPersonId = PersonTable.BusinessPersonId
    )
    , FctTableFinalJoin as (
        select
            AddressTable.StateProvinceAbbreviation
            , AddressTable.RegionName
            , AddressTable.CountryRegionCode
            , AddressTable.CountryName
            , AddressTable.AddressRoadName
            , AddressTable.AddressCityName
            , AddressTable.PostalCode
            , FctTableJoin4.*
        from FctTableJoin4
        left join AddressTable
            on FctTableJoin4.ShipToAddressId = AddressTable.PrincipalAddressId
    )

select *
from FctTableFinalJoin

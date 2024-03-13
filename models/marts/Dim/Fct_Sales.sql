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

    , SalesPerson as (
        select *
        from {{ ref('Sales_Person') }}
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
            AddressTable.StateProvinceAbbreviation
            , AddressTable.RegionName
            , AddressTable.CountryRegionCode
            , AddressTable.CountryName
            , AddressTable.AddressRoadName
            , AddressTable.AddressCityName
            , AddressTable.PostalCode
            , AddressTable.PrincipalAddressId
            , FctTableJoin2.*
        from FctTableJoin2
        left join AddressTable
            on FctTableJoin2.ShipToAddressId = AddressTable.PrincipalAddressId
    )

    , FctTableJoin4 as (
        select
            CreditCardTable.CardBrandType
            , CreditCardTable.BusinessPersonId
            , FctTableJoin3.*
        from FctTableJoin3
        left join CreditCardTable
            on FctTableJoin3.CreditCardId = CreditCardTable.CreditCardId
    )

    , FctTableFinalJoin as (
        select
            PersonTable.PersonTypeDescription
            , PersonTable.CompleteName
            , PersonTable.EmailAddressId
            , PersonTable.EmailAddress
            , PersonTable.AddressID
            , SalesPerson.SellerCompleteName
            , FctTableJoin4.*
        from FctTableJoin4
        left join PersonTable
            on FctTableJoin4.BusinessPersonId = PersonTable.BusinessPersonId
        left join SalesPerson
            on FctTableJoin4.SalesPersonId = SalesPerson.BusinessPersonId
    )

select *
from FctTableFinalJoin

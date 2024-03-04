with
    PersonTable as (
        select *
        from {{ ref('stg_erp_Person') }}
    )

    , AddressTable as (
        select *
        from {{ ref('stg_erp_Address') }}
    )

    , BusinessAddressTable as (
        select *
        from {{ ref('stg_erp_Business_Entity_Address') }}
    )

    , EmailAddressTable as (
        select *
        from {{ ref('stg_erp_Email_Address') }}
    )

    , FirstJoinTable as (
        select
            BusinessAddressTable.Pk_Address as Fk_BusinessAddress
            , BusinessAddressTable.AddressId
            , BusinessAddressTable.AddressTypeId
            , PersonTable.*
        from PersonTable
        inner join BusinessAddressTable
            on PersonTable.BusinessPersonId = BusinessAddressTable.BusinessPersonId
    )

    , SecondJoinTable as (
        select
            AddressTable.Pk_PrincipalAddress as Fk_PrincipalAddress
            , AddressTable.AddressRoadName
            , AddressTable.AddressCityName
            , AddressTable.StateProvinceId
            , AddressTable.PostalCode
            , FirstJoinTable.*
        from FirstJoinTable
        inner join AddressTable
            on FirstJoinTable.AddressId = AddressTable.AddressId
    )

    , ThirdJoinTable as (
        select
            EmailAddressTable.Pk_EmailAddress as Fk_EmailAddress
            , EmailAddressTable.EmailAddressId
            , EmailAddressTable.EmailAddress
            , SecondJoinTable.*
        from SecondJoinTable
        inner join EmailAddressTable
            on SecondJoinTable.BusinessPersonId = EmailAddressTable.BusinessPersonId
    )

    , NewPersonTable as (
        select *
        from ThirdJoinTable
    )

select *
from NewPersonTable

with
    PersonTable as (
        select *
        from {{ ref('stg_erp_Person') }}
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
        left join BusinessAddressTable
            on PersonTable.BusinessPersonId = BusinessAddressTable.BusinessPersonId
    )

    , SecondJoinTable as (
        select
            EmailAddressTable.Pk_EmailAddress as Fk_EmailAddress
            , EmailAddressTable.EmailAddressId
            , EmailAddressTable.EmailAddress
            , FirstJoinTable.*
        from FirstJoinTable
        left join EmailAddressTable
            on FirstJoinTable.BusinessPersonId = EmailAddressTable.BusinessPersonId
    )

    , NewPersonTable as (
        select *
        from SecondJoinTable
    )

select *
from NewPersonTable

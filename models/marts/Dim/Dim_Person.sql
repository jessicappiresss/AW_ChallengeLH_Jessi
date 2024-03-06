with
    PersonTable as (
        select *
        from {{ ref('Person') }}
    )

    , SelectingPersonTable as (
        select
            Pk_BusinessPerson
            , Fk_EmailAddress
            , BusinessPersonId
            , PersonType
            , PersonTypeDescription
            , ModelTypeName
            , TitleName 
            , CompleteName
            , EmailAddressId
            , EmailAddress
            , AddressID
            , AddressTypeId
        from PersonTable
    )

    , CreatingSkProduct as (
        select
            farm_fingerprint(cast(Pk_BusinessPerson as string)) as Sk_BusinessPerson
            , SelectingPersonTable.*
        from SelectingPersonTable
    )

select *
from CreatingSkProduct

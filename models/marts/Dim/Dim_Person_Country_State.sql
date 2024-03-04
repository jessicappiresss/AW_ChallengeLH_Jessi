with
    PersonTable as (
        select *
        from {{ ref('Person') }}
    )

    , CountryStateTable as (
        select *
        from {{ ref('Country_State') }}
    )

    , JoinTable as (
        select
            CountryStateTable.Fk_StateProvince
            , CountryStateTable.Fk_CountryRegion
            , CountryStateTable.StateProvinceAbbreviation
            , CountryStateTable.RegionName
            , CountryStateTable.CountryRegionCode
            , CountryStateTable.CountryName
            , CountryStateTable.TerritoryId
            , PersonTable.*
        from PersonTable
        inner join CountryStateTable
            on PersonTable.StateProvinceId = CountryStateTable.StateProvinceId
    )

    , NewPersonCountryStateTable as (
        select *
        from JoinTable
    )

    , SelectingPersonCountryStateTable as (
        select
            Pk_BusinessPerson
            , Fk_BusinessAddress
            , Fk_PrincipalAddress
            , Fk_EmailAddress
            , Fk_StateProvince
            , Fk_CountryRegion
            , BusinessPersonId
            , PersonType
            , PersonTypeDescription
            , ModelTypeName
            , TitleName 
            , CompleteName
            , AddressID
            , AddressTypeId
            , AddressRoadName
            , AddressCityName
            , StateProvinceId
            , PostalCode
            , EmailAddressId
            , EmailAddress
            , StateProvinceAbbreviation
            , RegionName
            , CountryRegionCode
            , CountryName
            , TerritoryId
        from NewPersonCountryStateTable
    )

    , CreatingSkProduct as (
        select
            farm_fingerprint(cast(Pk_BusinessPerson as string)) as Sk_BusinessPerson
            , SelectingPersonCountryStateTable.*
        from SelectingPersonCountryStateTable
    )

select *
from CreatingSkProduct

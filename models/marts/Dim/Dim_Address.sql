with
    AddressTable as (
        select *
        from {{ ref('Country_State') }}
    )

    , SelectingAddressTable as (
        select
            Pk_Territory
            , Fk_CountryRegion
            , Fk_PrincipalAddress
            , TerritoryId
            , StateProvinceId
            , StateProvinceAbbreviation
            , RegionName
            , CountryRegionCode
            , CountryName
            , AddressRoadName
            , AddressCityName
            , PostalCode
        from AddressTable
    )

    , CreatingSkProduct as (
        select
            farm_fingerprint(cast(Pk_Territory as string)) as Sk_Territory
            , SelectingAddressTable.*
        from SelectingAddressTable
    )

select *
from CreatingSkProduct

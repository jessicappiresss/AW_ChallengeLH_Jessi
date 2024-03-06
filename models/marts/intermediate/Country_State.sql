with
    StateProvinceTable as (
        select *
        from {{ ref('stg_erp_State_Province') }}
    )

    , CountryRegionTable as (
        select *
        from {{ ref('stg_erp_Country_Region') }}
    )

    , AddressTable as (
        select *
        from {{ ref('stg_erp_Address') }}
    )

    , FirstJoinTable as (
        select
            StateProvinceTable.Pk_Territory
            , CountryRegionTable.Pk_CountryRegion as Fk_CountryRegion
            , StateProvinceTable.StateProvinceId
            , StateProvinceTable.StateProvinceAbbreviation
            , StateProvinceTable.RegionName
            , StateProvinceTable.CountryRegionCode
            , CountryRegionTable.CountryName
            , StateProvinceTable.TerritoryId
        from StateProvinceTable
        left join CountryRegionTable
            on StateProvinceTable.CountryRegionCode = CountryRegionTable.CountryRegionCode
    )

    , SecondJoinTable as (
        select
            AddressTable.Pk_PrincipalAddress as Fk_PrincipalAddress
            , AddressTable.AddressRoadName
            , AddressTable.AddressCityName
            , AddressTable.PostalCode
            , FirstJoinTable.*
        from FirstJoinTable
        inner join AddressTable
            on FirstJoinTable.StateProvinceId = AddressTable.StateProvinceId
    )

    , NewCountryStateTable as (
        select *
        from SecondJoinTable
    )

select *
from NewCountryStateTable

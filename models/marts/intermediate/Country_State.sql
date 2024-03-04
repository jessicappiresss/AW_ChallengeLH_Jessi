with
    StateProvinceTable as (
        select *
        from {{ ref('stg_erp_State_Province') }}
    )

    , CountryRegionTable as (
        select *
        from {{ ref('stg_erp_Country_Region') }}
    )

    , FirstJoinTable as (
        select
            StateProvinceTable.Pk_StateProvince as Fk_StateProvince
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

    , NewCountryStateTable as (
        select *
        from FirstJoinTable
    )

select *
from NewCountryStateTable

with
    ProductSubcategory as (
        select *
        from {{ ref('stg_erp_Product_Subcategory') }}
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(ProductsubcategoryId as string)) as Pk_Productsubcategory
            , *
        from ProductSubcategory
    )

select *
from CreatingPk

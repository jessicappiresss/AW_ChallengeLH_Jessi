with
    ProductSubcategory as (
        select *
        from {{ ref('stg_erp_Product_Subcategory') }}
    )

    , CreatingPk as (
        select
            xxhash64(ProductsubcategoryId) as Pk_Productsubcategory
            , *
        from ProductSubcategory
    )

select *
from CreatingPk
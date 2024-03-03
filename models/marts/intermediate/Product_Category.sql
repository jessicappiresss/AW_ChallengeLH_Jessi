with
    ProductCategory as (
        select *
        from {{ ref('stg_erp_Product_Category') }}
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(ProductCategoryId as string)) as Pk_ProductCategory
            , *
        from ProductCategory
    )

select *
from CreatingPk

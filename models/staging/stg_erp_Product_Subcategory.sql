with
    ProductSubcategoryStaging as (
        select
            cast(productsubcategoryid as int ) as ProductsubcategoryId
            , cast(productcategoryid as int) as ProductCategoryCode
            , cast(name as string) as ProductSubCategoryName
        from {{ source('production', 'productsubcategory') }}
    )

select *
from ProductSubcategoryStaging
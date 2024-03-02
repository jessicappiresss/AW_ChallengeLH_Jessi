with
    ProductCategoryStaging as (
        select
            cast(productcategoryid as int) as ProductCategoryId
            , cast(name as string) as ProductCategoryName
        from {{ source('production', 'productcategory') }}
    )

select *
from ProductCategoryStaging
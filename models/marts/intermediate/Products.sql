with
    ProductPrincipalTable as (
        select *
        from {{ ref('stg_erp_Product') }}
    )

    , ProductCategory as (
        select *
        from {{ ref('Product_Category') }}
    )

    , ProductSubcategory as (
        select *
        from {{ ref('Product_Subcategory') }}
    )

    , NewProductTableFirstJoin as (
        select
            ProductSubcategory.ProductSubcategoryId
            , ProductSubcategory.ProductSubCategoryName
            , ProductSubcategory.Pk_Productsubcategory
            , ProductSubcategory.ProductCategoryCode
            , ProductPrincipalTable.*
        from ProductPrincipalTable
        left join ProductSubcategory
            on ProductPrincipalTable.ProductSubcategoryCode 
            = ProductSubcategory.ProductSubcategoryId
    )

    , NewProductTable as (
        select
            ProductCategory.ProductCategoryId
            , ProductCategory.ProductCategoryName
            , ProductCategory.Pk_ProductCategory
            , NewProductTableFirstJoin.*
        from NewProductTableFirstJoin
        left join ProductCategory
            on NewProductTableFirstJoin.ProductCategoryCode
            = ProductCategory.ProductCategoryId
    )

    , CreatingPkProduct as (
        select
            farm_fingerprint(cast(ProductId as string)) as Pk_Product
            , Pk_Productsubcategory as Fk_Productsubcategory
            , Pk_ProductCategory as Fk_ProductCategory
            , NewProductTable.*
        from NewProductTable
    )

select *
from CreatingPkProduct

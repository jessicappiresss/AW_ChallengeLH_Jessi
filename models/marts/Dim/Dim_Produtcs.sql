with
    ProductPrincipalTable as (
        select *
        from {{ ref('Products') }}
    )

    , SelectingColumnsDimProducts as (
        select
            Pk_Product
            , Fk_Productsubcategory
            , Fk_ProductCategory
            , ProductId
            , ProductName
            , ProductCategoryName
            , ProductSubCategoryName
            , ProductInternalCode
            , ManufactureTypes
            , ReadyForSale
            , ProductColor
            , ProductMinimumStockQuantity
            , ProductReorderPoint
            , ProductStandardCost
            , ProductSellPrice
            , ProductSize
            , ManufactureProductInDays
            , ProductLineType
            , ProductClass
            , ProductStyle
            , ProductSubcategoryCode
            , ProductModelCode
            , SellStartDate
            , ProductUnavailableDate
        from ProductPrincipalTable
    )

    , CreatingSkProduct as (
        select
            farm_fingerprint(cast(Pk_Product as string)) as Sk_Products
            , SelectingColumnsDimProducts.*
        from SelectingColumnsDimProducts
    )

select *
from CreatingSkProduct

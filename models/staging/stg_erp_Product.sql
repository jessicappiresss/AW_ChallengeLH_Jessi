with 
    ProductStaging as (
        select
            cast(productid as int) as ProductId
            , cast(inicap(name as string)) as ProductName
            , cast(productnumber as string) ProductInternalCode
            , cast(makeflag as string) as ManufactureTypes
            case 
                when makeflag = true
                    then 'The product is purchased'
                when makeflag = false
                    then 'The product is internal manufactured'
                else 'Unknown'
            end as ManufactureType
            , cast(finishedgoodsflag as string) as FinishedProductsFlag
            case 
                when finishedgoodsflag = true
                    then 'Yes'
                when finishedgoodsflag = false
                    then 'No'
                else 'Unknown'
            end as ReadyForSale
            , cast(color as string) as ProductColor
            , cast(safetystocklevel as int) as ProductMinimumStockQuantity
            , cast(reorderpoint as int) as ProductReorderPoint
            , cast(standardcost as float64) as ProductStandardCost
            , cast(listprice as float64) as ProductSellPrice
            , cast(size as string) as ProductSize
            , cast(daystomanufacture as int) as ManufactureProductInDays
            , cast(productline as string) as ProductLineType
            case
                when productline = 'R'
                    then 'Road'
                when productline = 'M'
                    then 'Mountain'
                when productline = 'T'
                    then 'Touring'
                when productline = 'S'
                    then 'Standard'
                else 'Unknown'
            end as ProductLineType
            , cast(class as string) as ProductClass
            case
                when class = 'H'
                    then 'High'
                when class = 'M'
                    then 'Medium'
                when class = 'L'
                    then 'Low'
                else 'Unknown'
            end as ProductClass
            , cast(style as string) case ProductStyle
                when style = 'W'
                    then 'Feminine'
                when style = 'M'
                    then 'Masculine'
                when style = 'U'
                    then 'Universal'
                else 'Unknown'
            end as ProductStyle
            , cast(productsubcategoryid as int) as ProductSubcategoryId
            , cast(productmodelid as int) as ProductModelId
            , cast(sellstartdate as timestamp) as SellStartDate
            , cast(sellenddate as timestamp) as ProductUnavailableDate
        from {{ source('production', 'product') }}
    )

Select *
from ProductStaging
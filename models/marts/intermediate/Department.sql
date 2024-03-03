with
    Department as (
        select *
        from {{ ref('stg_erp_Department') }}
    )

    , CreatingPk as (
        select
            farm_fingerprint(cast(DepartmentEmployeeId as string)) as Pk_DepartmentEmployee
            , DepartmentEmployeeId
            , DepartamentName
            , DepartmentGroupName
        from Department
    )

select *
from CreatingPk

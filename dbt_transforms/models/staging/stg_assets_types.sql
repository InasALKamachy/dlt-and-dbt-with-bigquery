with

source as (

    select * from {{ source('fortnox_raw', 'assets_types') }}

),

renamed as (

    select
        *
    from source

)

select * from renamed
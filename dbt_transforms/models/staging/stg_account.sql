with

source as (

    select * from {{ source('fortnox_raw', 'accounts') }}

),

renamed as (

    select
        *
    from source

)

select * from renamed
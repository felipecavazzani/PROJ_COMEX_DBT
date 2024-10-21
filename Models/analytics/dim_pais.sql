{{ config(materialized='incremental') }}

WITH source AS (
    SELECT DISTINCT
        fc.co_pais,
        fc.co_isoa3,
        (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_carga  
    FROM 
        {{ ref('fct_comex_desnormalized') }} fc 
    WHERE 
        fc.co_pais IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY co_pais) AS pais_sk,
    co_pais,
    co_isoa3,
    data_carga
FROM source

{% if is_incremental() %}
WHERE co_pais NOT IN (SELECT co_pais FROM {{ this }})
{% endif %}
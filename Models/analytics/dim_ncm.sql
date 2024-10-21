{{ config(materialized='incremental') }}

WITH source AS (
    SELECT DISTINCT
        fc.co_ncm,
        fc.nome_ncm,
        (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_carga  
    FROM 
        {{ ref('fct_comex_desnormalized') }} fc 
    WHERE 
        fc.co_ncm IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY co_ncm) AS ncm_sk,
    co_ncm,
    nome_ncm,
    data_carga
FROM source

{% if is_incremental() %}
WHERE co_ncm NOT IN (SELECT co_ncm FROM {{ this }})
{% endif %}

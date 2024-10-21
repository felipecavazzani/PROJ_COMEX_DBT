{{ config(materialized='incremental') }}

WITH source AS (
    SELECT DISTINCT
        fc.co_via,
        fc.nome_via,
        (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_carga  
    FROM 
        {{ ref('fct_comex_desnormalized') }} fc 
    WHERE 
        fc.co_via IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY co_via) AS via_sk,
    co_via,
    nome_via,
    data_carga
FROM source

{% if is_incremental() %}
WHERE co_via NOT IN (SELECT co_via FROM {{ this }})
{% endif %}
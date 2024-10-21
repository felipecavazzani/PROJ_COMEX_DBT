{{ config(materialized='incremental') }}

WITH source AS (
    SELECT DISTINCT
        fc.co_uf,
        fc.sg_uf,
        fc.no_uf,
        fc.no_regiao,
        (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_carga  
    FROM 
        {{ ref('fct_comex_desnormalized') }} fc 
    WHERE 
        fc.co_uf IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY co_uf) AS uf_sk,
    co_uf,
    sg_uf,
    no_uf,
    no_regiao,
    data_carga
FROM source

{% if is_incremental() %}
WHERE co_uf NOT IN (SELECT co_uf FROM {{ this }})
{% endif %}
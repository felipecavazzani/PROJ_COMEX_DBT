
{{config(materialized='incremental')}}

WITH source AS (
SELECT 
"CO_UF" as co_uf, 
"SG_UF" as sg_uf, 
"NO_UF" as no_uf, 
"NO_REGIAO" as no_regiao,
(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS "data_carga"
FROM {{source('sources', 'uf')}}

)

SELECT
    co_uf,
    sg_uf,
    no_uf,
    no_regiao,
    data_carga
FROM source

{% if is_incremental() %}
WHERE co_uf NOT IN (SELECT co_uf FROM {{this}})
{% endif %}

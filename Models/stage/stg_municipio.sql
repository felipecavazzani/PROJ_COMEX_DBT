{{config(materialized='incremental')}}

WITH source AS (
    SELECT 
        "CO_MUN_GEO" as cod_mun, 
        "NO_MUN" as nome_mun, 
        "NO_MUN_MIN" as nome_mun_min, 
        "SG_UF" as uf,
        (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS "data_carga"
    FROM {{source('sources', 'mun')}}
)


SELECT
    cod_mun,
    nome_mun,
    nome_mun_min,
    uf,
    data_carga
FROM source

{% if is_incremental() %}
WHERE cod_mun NOT IN (SELECT cod_mun FROM {{this}})
{% endif %}

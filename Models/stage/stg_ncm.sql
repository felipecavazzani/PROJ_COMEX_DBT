{{config(materialized='incremental')}}

WITH source AS (
SELECT 
	"CO_NCM" as co_ncm,  
	"NO_NCM_POR" as nome_ncm,
	(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS "data_carga"
FROM {{source('sources', 'ncm')}}
)

SELECT
    co_ncm,
    nome_ncm,
    data_carga
FROM source

{% if is_incremental() %}
WHERE co_ncm NOT IN (SELECT co_ncm FROM {{this}})
{% endif %}

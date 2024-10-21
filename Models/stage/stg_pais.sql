{{config(materialized='incremental')}}

WITH source AS (
SELECT 
	"CO_PAIS" as co_pais, 
	"CO_PAIS_ISON3" as co_ison3, 
	"CO_PAIS_ISOA3" as co_isoa3, 
	"NO_PAIS" as nome_pais,
	(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS "data_carga"
FROM {{source('sources', 'pais')}}
)
SELECT
    co_pais,
    co_ison3,
    co_isoa3,
    data_carga
FROM source

{% if is_incremental() %}
WHERE co_pais NOT IN (SELECT co_pais FROM {{this}})
{% endif %}
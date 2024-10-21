WITH valida_pais AS (
    SELECT
        pais_sk,
        COUNT(*) AS conta_duplicados,
        SUM(CASE WHEN co_pais IS NULL  OR co_isoa3 IS NULL OR data_carga IS NULL THEN 1 ELSE 0 END) AS conta_nulos
    FROM {{ ref('dim_pais') }} 
    GROUP BY pais_sk
)

SELECT *
FROM valida_pais
WHERE conta_duplicados > 1
   OR conta_nulos > 0

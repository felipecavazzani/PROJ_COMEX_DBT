WITH valida_ncm AS (
    SELECT
        ncm_sk,
        COUNT(*) AS conta_duplicados,
        SUM(CASE WHEN co_ncm IS NULL OR nome_ncm IS NULL OR data_carga IS NULL THEN 1 ELSE 0 END) AS conta_nulos
    FROM {{ ref('dim_ncm') }} 
    GROUP BY ncm_sk
)

SELECT *
FROM valida_ncm
WHERE conta_duplicados > 1
   OR conta_nulos > 0
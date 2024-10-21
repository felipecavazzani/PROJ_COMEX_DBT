WITH valida_via AS (
    SELECT
        via_sk,
        COUNT(*) AS cont_duplicado,
        SUM(CASE WHEN co_via IS NULL OR nome_via IS NULL OR data_carga IS NULL THEN 1 ELSE 0 END) AS counta_nulos
    FROM {{ ref('dim_via') }}  
    GROUP BY via_sk
)

SELECT *
FROM valida_via
WHERE cont_duplicado > 1
   OR counta_nulos > 0
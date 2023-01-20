WITH

IVR_database AS (
SELECT  *
        ,DATE_TRUNC('month',date(dt)) AS month
        ,lower(intent)
        ,CASE WHEN lower(intent) LIKE '%tech%' THEN 'Technical' ELSE 'Other' END AS interaction_purpouse
FROM "lla_polaris"."polarisivr_cwc"
WHERE OPCO='CWC'
    AND TRY_CAST(variable1 AS BIGINT) IS NOT NULL
)

/*
SELECT  month
        ,interaction_purpouse
        ,COUNT(DISTINCT callreferenceid)
FROM IVR_database
GROUP BY 1,2
*/

,summary AS (
SELECT  C.*, customers_calling
FROM    (SELECT A.*, all_interactions
        FROM    (SELECT month
                        ,COUNT(DISTINCT callreferenceid) AS tech_interactions
                FROM IVR_database
                WHERE interaction_purpouse = 'Technical'
                GROUP BY 1) A 
        LEFT JOIN 
        (SELECT month
                ,COUNT(DISTINCT callreferenceid) AS all_interactions
        FROM IVR_database
        GROUP BY 1) B
        ON A.month = B.month) C 
LEFT JOIN
(SELECT month
        ,COUNT(DISTINCT variable1) AS customers_calling
FROM IVR_database
GROUP BY 1) D
ON C.month = D.month
)

SELECT *
        ,ROUND(CAST(tech_interactions AS DOUBLE)/CAST(all_interactions AS DOUBLE)*100,2) AS tech_calls_intensity_with_all_interactions
        ,ROUND(CAST(tech_interactions AS DOUBLE)/CAST(customers_calling AS DOUBLE)*100,2) AS tech_calls_intensity_with_cust_calling
FROM summary
ORDER BY 1

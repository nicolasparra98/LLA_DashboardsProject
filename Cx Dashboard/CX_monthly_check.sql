SELECT A.*, B.KPI_MEAS AS DEC_MEAS
        FROM    (SELECT month, journey_waypoint, facet, kpi_name, kpi_meas AS NOV_MEAS
                FROM resultados 
                WHERE month=date('2022-11-01') AND Network = 'OVERALL'
                ) A LEFT JOIN 
                (SELECT month, kpi_name, kpi_meas 
                FROM resultados 
                where month=date('2022-12-01') and Network = 'OVERALL'
                ) B ON A.KPI_NAME = B.KPI_NAME

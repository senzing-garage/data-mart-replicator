SELECT t1.data_source1 AS data_source, 
       t1.entity_count AS rpt_entity_count, 
       t1.record_count AS rpt_record_count, 
       t2.entity_count AS det_entity_count,
       t4.entity_count AS mrt_entity_count,
       t3.record_count AS mrt_record_count
FROM sz_dm_report AS t1 
FULL OUTER JOIN (
    SELECT report_key, COUNT(*) AS entity_count FROM sz_dm_report_detail GROUP BY report_key
) AS t2 
ON t1.report_key = t2.report_key
FULL OUTER JOIN (
    SELECT data_source, COUNT(*) AS record_count FROM sz_dm_record GROUP BY data_source
) AS t3
ON t1.data_source1 = t3.data_source
FULL OUTER JOIN (
    SELECT data_source, COUNT(DISTINCT(entity_id)) AS entity_count 
    FROM (SELECT rec.data_source AS data_source, rec.entity_id AS entity_id FROM sz_dm_record AS rec 
          FULL OUTER JOIN sz_dm_entity AS ent ON rec.entity_id = ent.entity_id) 
    GROUP BY data_source
) AS t4
ON t1.data_source1 = t4.data_source
WHERE t1.report='DSS' AND t1.statistic='ENTITY_COUNT';
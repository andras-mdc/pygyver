`staging.table1` AS (
    SELECT 'A001' AS id,'102461350' AS order_reference,'sofa' AS sku UNION ALL
    SELECT 'A002','1600491918','chair'
  ),
  `staging.table2` AS (
    SELECT 'A001' AS id, 100 AS price UNION ALL
    SELECT 'A002', 200
  ),
  `expected_output` AS (
    SELECT 'A001' AS id,'102461350' AS order_reference,'sofa' AS sku, 100 AS price UNION ALL
    SELECT 'A002','1600491918','chair', 100)
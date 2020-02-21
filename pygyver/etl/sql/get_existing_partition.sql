SELECT
    FORMAT_DATE('%Y%m%d', DATE(_PARTITIONTIME)) as partition_id
FROM
    {dataset_id}.{table_id}
GROUP BY
    1

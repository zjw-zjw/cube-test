SELECT *
FROM `fact_order_retail_offline` AS `retail_order`
LEFT JOIN LATERAL TABLE(`order_dim_date_time`(`retail_order`.`submit_datetime`)) AS `submit_datetime` ON TRUE
LEFT JOIN `mysql`.`cube`.`dim_address` AS `dim_address` ON `dim_address`.`address_id` = `retail_order`.`address_id`
LEFT JOIN `mysql`.`cube`.`dim_ware` AS `dim_ware` ON `dim_ware`.`ware_id` = `retail_order`.`ware_id`
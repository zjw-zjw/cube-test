select
    date_format(ordertime, 'yyyy-MM-dd') as submit_date,
    detailid as id,
    warecode as ware_id,
    ordercode as order_id,
    ordertime as order_datetime,
    ordertime as submit_datetime
from
    `hive`.`test_hive`.`ods_om_om_orderdetail_base`
limit 100
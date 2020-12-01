select
    a.ordercode,
    b.orderstatus,
    b.createdate
from
    hive a
join
    ods_om_om_orderaddress b
on
    a.ordercode = b.ordercode
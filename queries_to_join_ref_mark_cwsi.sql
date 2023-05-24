WITH devmap AS (
  SELECT *
  FROM (VALUES
('D003701', 'TWE_GB', 'L1'),
('D003705', 'TWE_GB', 'L2'),
('D003932', 'TWE_GB', 'H1'),
('D003978', 'TWE_GB', 'H2'),
('D003898', 'TWE_BV2', 'L1'),
('D003960', 'TWE_BV2', 'L2'),
('D003942', 'TWE_BV2', 'H1'),
('D003943', 'TWE_BV2', 'H2')
) AS t(device, site_id, source)
), 
cte as (
select
 	   DATE_TRUNC('hour', ref_time) as ref_time, 
       site_id,
       source,
       avg(ref_tbelow) as ref_tbelow,
       avg(ref_tsensor) as body_temp

FROM   device_data.calval_ref_data
where site_id in ('TWE_GB', 'TWE_BV2')
and ref_time>'2023-3-25'
group by ref_time, site_id, source
ORDER  BY site_id, source, ref_time  
),
cte1 as (
SELECT c.*, d.device FROM devmap d
join cte c
using (site_id, source)
)
SELECT time, tair, tbelow, vpd, ea, precip, c.* 
from device_data_alp.hourly d
join cte1 c
on c.device=d.device and c.ref_time =d.time
--
where 

d.device  in (
'D003701', 
'D003705', 
'D003932', 
'D003978', 
'D003898', 
'D003960', 
'D003942', 
'D003943' )
order by time
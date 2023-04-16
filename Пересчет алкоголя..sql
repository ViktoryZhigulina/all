--Вывожу алкоголь из подневного прогноза
select a.*, b.date_recalc_from, b.date_recalc_till
into ##zhv_alco
from (select * from daily.result where link_type in ('а','р')) a
join sandbox.load_dc.zhv_recalc_alco b
on a.di=b.di

--drop table ##zhv_alco
--drop table ##zhv_alco_delete_sum
--update ##zhv_alco_1 set data_astr = dateadd(m,+1,data_astr)
--update ##zhv_alco set data_astr = dateadd(m,+2,data_astr) where data_astr between '15.01.2023' and '28.01.2023'
select distinct data_astr from ##zhv_alco order by 1

--Вывожу связки и сумму удаляемого объема в дни пересчета:
select di
      ,filial_di
	  ,format_di
	  ,whs_di
	  ,delivery_type
	  ,link_type
	  ,group_20_di
	  ,box
	  ,sum(box) over (partition by di,filial_di,format_di,delivery_type,link_type,group_20_di) box_delete
	  ,week
	  ,year
	  ,data_astr
	  ,proc_di
	  ,date_recalc_from
	  ,date_recalc_till
into ##zhv_alco_delete_sum
from ##zhv_alco
where data_astr >= date_recalc_from and data_astr <= date_recalc_till

--drop table ##zhv_alco_delete_sum
select * from ##zhv_alco_delete_sum order by 12

--Рассчитываю суммы с учетом переноса объемов в дни расчёта:
select a.*
      ,b.box_delete
	  ,box*(case when a.sum_box !=0 and box_delete is not null and (a.data_astr = dateadd(d,-1,a.date_recalc_from) or data_astr = dateadd(d,+2,date_recalc_till)) then (a.sum_box+b.box_delete*0.3)/a.sum_box
	        when a.sum_box !=0 and box_delete is not null and data_astr = dateadd(d,+1,date_recalc_till) then (a.sum_box+b.box_delete*0.4)/a.sum_box
			else 1 end) as box_new
into ##zhv_alco_recalc_sum
from
(select a1.*,sum(box) over (partition by di,filial_di,format_di,delivery_type,link_type,group_20_di) as sum_box, 1 as flag
from ##zhv_alco as a1
where data_astr = dateadd(d,-1,date_recalc_from)
union
select a2.*,sum(box) over (partition by di,filial_di,format_di,delivery_type,link_type,group_20_di) as sum_box, 2 as flag
from ##zhv_alco as a2
where data_astr = dateadd(d,+1,date_recalc_till)
union
select a3.*,sum(box) over (partition by di,filial_di,format_di,delivery_type,link_type,group_20_di) sum_box, 3 as flag
from ##zhv_alco as a3
where data_astr = dateadd(d,+2,date_recalc_till)) as a
left join (select distinct di,filial_di,format_di,delivery_type,link_type,group_20_di, box_delete from ##zhv_alco_delete_sum) b
on a.di=b.di and a.filial_di=b.filial_di and a.format_di=b.format_di and a.delivery_type=b.delivery_type and a.link_type=b.link_type and a.group_20_di=b.group_20_di

select * from ##zhv_alco_recalc_sum
--drop table ##zhv_alco_recalc_sum

select sum(box) from ##zhv_alco where data_astr >= date_recalc_from and data_astr <= date_recalc_till
select sum(box_new)-sum(box) from ##zhv_alco_recalc_sum

select distinct di,filial_di,format_di,delivery_type,link_type,group_20_di from ##zhv_alco_delete_sum
except
select distinct di,filial_di,format_di,delivery_type,link_type,group_20_di from ##zhv_alco where data_astr < date_recalc_from or data_astr > date_recalc_till

select distinct di,filial_di,format_di,delivery_type,link_type,group_20_di from ##zhv_alco where data_astr < date_recalc_from or data_astr > date_recalc_till
except
select distinct di,filial_di,format_di,delivery_type,link_type,group_20_di from ##zhv_alco_delete_sum

select di, sum(c)
from
(
select di,filial_di,format_di,delivery_type,link_type,group_20_di,box_delete, sum(box)+box_delete a, sum(box_new) b, (sum(box_new) - (sum(box)+box_delete)) c from ##zhv_alco_recalc_sum
group by di,filial_di,format_di,delivery_type,link_type,group_20_di,box_delete
) a1
group by di

--delete from ##zhv_alco where data_astr >= date_recalc_from and data_astr <= date_recalc_till
--delete from ##zhv_alco where data_astr = dateadd(d,-1,date_recalc_from) or data_astr = dateadd(d,+1,date_recalc_till) or data_astr = dateadd(d,+2,date_recalc_till)

insert into ##zhv_alco
select di
      ,filial_di
	  ,format_di
	  ,whs_di
	  ,delivery_type
	  ,link_type
	  ,group_20_di
	  ,box_new as box
	  ,week
	  ,year
	  ,data_astr
	  ,proc_di
	  ,date_recalc_from
	  ,date_recalc_till
from ##zhv_alco_recalc_sum

--Проверка:
select sum(box) from ##zhv_alco
select sum(box) from daily.result where link_type in ('а','р')

--Удаление старых, заливка новых связок:
--delete from daily.result where link_type in ('а','р')

insert into daily.result
select di
      ,filial_di
	  ,format_di
	  ,whs_di
	  ,delivery_type
	  ,link_type
	  ,group_20_di
	  ,box
	  ,week
	  ,year
	  ,data_astr
	  ,proc_di
from ##zhv_alco

select di,data_astr,sum(box) from ##zhv_alco group by di,data_astr
select di,data_astr,sum(box) from daily.result where link_type in ('а','р') group by di,data_astr

------------------------------------------------------
with a1 as        
 (SELECT    di
      ,filial_di
	  ,format_di
	  ,whs_di
	  ,delivery_type
	  ,link_type
	  ,group_20_di
	  ,week
	  ,year
	  ,data_astr
	   ,ROW_NUMBER() OVER(PARTITION BY  di
      ,filial_di
	  ,format_di
	  ,whs_di
	  ,delivery_type
	  ,link_type
	  ,group_20_di
	  ,week
	  ,year
	  ,data_astr ORDER BY data_astr) AS rn
	FROM ##zhv_alco
	GROUP BY di
	  ,filial_di
	  ,format_di
	  ,whs_di
	  ,delivery_type
	  ,link_type
	  ,group_20_di
	  ,week
	  ,year
	  ,data_astr)
select *
from a1 where rn > 1

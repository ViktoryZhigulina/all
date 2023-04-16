--Создание отсутствующих связок:
select a.*
into ##plus_empty
from
(select distinct di
                ,filial_di
		        ,format_di
		        ,a.whs_di
		        ,delivery_type
		        ,a.week
		        ,year
				,proc_di
from weekly.result as a
join (select whs_di from bakery) as b
on a.whs_di=b.whs_di and a.delivery_type='ПД'
except
select distinct di
               ,filial_di
		       ,format_di
		       ,whs_di
		       ,delivery_type
		       ,week
		       ,year
			   ,proc_di
from weekly.result
where delivery_type='ПД' and group_20_di=679 and link_type='з') a

--drop table ##plus_empty

insert into weekly.result
select di
      ,filial_di
	  ,format_di
	  ,whs_di
	  ,delivery_type
	  ,'з'as link_type
	  ,679 as group_20_di
	  ,0 as box
	  ,week
	  ,year
	  ,proc_di
from ##plus_empty

--Апдейт пекарен:
DECLARE @Week INT = (select min(week)-1 from weekly.result where year = 2023);
update weekly.result
set box = box + 139.3
from weekly.result as a
left join bakery as b on a.whs_di=b.whs_di
where ((a.week > = b.week and b.week > = @Week and year = 2023) or
      (b.week > = @Week and year = 2024))
	  and group_20_di=679 and delivery_type='ПД' and link_type='з'
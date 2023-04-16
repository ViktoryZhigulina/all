/*������ ������ �������*/
--insert 
SELECT a1.*
      ,a2.rc_name rc
	  ,a3.group_20
	  ,CAST(GETDATE() as date) date_add
into result_without_perevods
FROM weekly a1
LEFT JOIN correct_dic a2 ON a1.di=a2.di
LEFT JOIN (SELECT DISTINCT group_20,group_20_di FROM articles) a3
ON a1.group_di=a3.group_di
WHERE rc_name IS NOT NULL

/*�������� (�� ����������� 12%)*/
--drop table ##perevods_2
--������ ������ �� ������� ��� �������������:
DECLARE @Week INT = (select min(week)+1 from result where year = 2023);
select a.*
into ##perevods_1
from ( --��������� ������ � ������� > ������ �������� ��� ������ ��������:
SELECT a1.*
      ,a2.di_old
	  ,a2.di_new
	  ,a2.di_home
	  ,a2.strategy
	  ,1 as flag
	  ,a1.box as box_new
FROM result a1
JOIN perevods a2 ON a1.whs_di=a2.whs_di AND ((a1.week>a2.week and year = 2023) or year = 2024) AND a1.di = a2.di_old and a1.week>@Week
union all--��������� ������ ������ ��������, ������� ������ �� ����������� ��� ������ �� ����� ��:
SELECT a1.*
      ,a2.di_old
	  ,a2.di_new
	  ,a2.di_home
	  ,a2.strategy
	  ,2 as flag
	  ,a1.box*a2.koef_week as box_new
FROM result a1
JOIN perevods a2 ON a1.whs_di=a2.whs_di AND a1.week=a2.week and year = 2023 AND a1.di = a2.di_old and a1.week>@Week
union all --��������� ������ ������ ��������, ������� ������ �� ����������� ��� ���������� ������� ��:
SELECT a1.*
      ,a2.di_old
	  ,a2.di_new
	  ,a2.di_home
	  ,a2.strategy
	  ,3 as flag
	  ,a1.box*(1-a2.koef_week) as box_new
FROM result a1
JOIN perevods a2 ON a1.whs_di=a2.whs_di AND a1.week=a2.week and year = 2023 AND a1.di = a2.di_old and a1.week>@Week
union all --��������� ������ ��� ����������� ������������ � ��������� ������:
SELECT a1.*
      ,a2.di_old
	  ,a2.di_new
	  ,a2.di_home
	  ,a2.strategy
	  ,4 as flag
	  ,a1.box as box_new
FROM result a1
JOIN perevods a2 ON a1.whs_di=a2.whs_di AND ((a1.week>=a2.week and year = 2023) or year = 2024) AND a1.di = a2.di_new and a1.week>@Week) a

--����� ��� �������� � ��:
update ##perevods_1 set delivery_type = '��' where (di_new = di_home) and strategy not like '%12% �� ����������%' and flag in (1,2)
update ##perevods_1 set di = di_new where di != di_new and strategy not like '%12% �� ����������%' and strategy not like '%�������� ���������� ���� �� ����������%' and flag in (1,2)
update ##perevods_1 set di = di_new where di != di_new and strategy not like '%12% �� ����������%' and strategy like '%�������� ���������� ���� �� ����������%' and delivery_type = '��' and flag in (1,2)
update ##perevods_1 set delivery_type = '��' where strategy  like '%������������ �� ��%'

--�������� ������� ��� �������� �� ���������:
with a1 as        
 (SELECT    di,
            whs_di,
			week,
			delivery_type,
			year,
			ROW_NUMBER() OVER(PARTITION BY  di, whs_di, week, year ORDER BY whs_di) AS rn
		FROM ##perevods_1
		GROUP BY di, whs_di, week, delivery_type, year)
select *
into ##update_perevods_1
from a1 where rn > 1

--drop table ##update_perevods_1

--������ ��� ������������� �������� ���� ��������:
update ##perevods_1
set delivery_type = '��'
from ##perevods_1 as a
join ##update_perevods_1 as b
on a.di=b.di and a.whs_di=b.whs_di and a.week=b.week and a.year=b.year
------------------------------------------------
--��������:
with c as
(select *
from ##perevods_1 as a
join (SELECT di as cntr,
                  whs_di as whs,
			      week as week_i,
				  delivery_type as delivery,
				  year as year_i,
				  ROW_NUMBER() OVER(PARTITION BY  di, whs_di, week, year ORDER BY whs_di) AS rn
			 FROM ##perevods_1
		 GROUP BY di, whs_di, week, delivery_type, year) as b
			   on a.di=b.cntr and a.whs_di=b.whs and a.week=b.week_i and a.year=b.year_i
			where rn > 1)
select distinct di,
                  whs_di,
			      week,
				  delivery_type,
				  year
from c
---------------------------------------------------------------------------

/*������� 12%*/

SELECT*FROM ##perevods_1 where strategy like '%12% �� ����������%'
--drop table ##perevods

--����� �� � ��� �������� 12% ������ ������������: 
SELECT di_new di
      ,filial_di
      ,format_di
	  ,whs_di
	  ,'��' delivery_type
	  ,link_type
	  ,group_20_di
	  ,box
	  ,week
	  ,year
	  ,proc_di
	  ,di_old
	  ,di_new
	  ,di_home
	  ,strategy
	  ,flag
	  ,box_new*0.12 box_new
into ##perevods_2
FROM ##perevods_1
WHERE strategy like '%12% �� ����������%' and delivery_type='��' and link_type NOT IN ('�','�','�','�') and flag in (1,2)

--�������� 88% ������ ������������:
insert into ##perevods_2
SELECT di
      ,filial_di
      ,format_di
	  ,whs_di
	  ,delivery_type
	  ,link_type
	  ,group_20_di
	  ,box
	  ,week
	  ,year
	  ,proc_di
	  ,di_old
	  ,di_new
	  ,di_home
	  ,strategy
	  ,flag
	  ,box_new*0.88 box_new
FROM ##perevods_1
WHERE strategy like '%12% �� ����������%' and delivery_type='��' and link_type NOT IN ('�','�','�','�') and flag in (1,2)

delete from ##perevods_1 WHERE strategy like '%12% �� ����������%' and delivery_type='��' and link_type NOT IN ('�','�','�','�') and flag in (1,2)

insert into ##perevods_1
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
	  ,proc_di
	  ,di_old
	  ,di_new
	  ,di_home
	  ,strategy
	  ,flag
	  ,box_new
FROM ##perevods_2

select di
      ,filial_di
      ,format_di
	  ,whs_di
	  ,delivery_type
	  ,link_type
	  ,group_20_di
	  ,sum(box_new) box
	  ,week
	  ,year
	  ,proc_di
into ##perevods
FROM ##perevods_1
group by  di,filial_di,format_di,whs_di,delivery_type,link_type,group_20_di,week,year,proc_di

--�������� ������ �� ������� ��� �������������:
select sum(box) from ##perevods

DECLARE @Week INT = (select min(week)+1 from result where year = 2023);
SELECT sum(box)
FROM result a1
JOIN sandbox.load_dc.zhv_perevods a2 ON a1.whs_di=a2.whs_di AND ((a1.week>=a2.week and year = 2023) or year = 2024) AND (a1.di = a2.di_old or a1.di = di_new) and a1.week>@Week

--������ ������ �� ������� ��� ������������� � ������� ���������:
DECLARE @Week INT = (select min(week)+1 from result where year = 2023);
delete a1
FROM result a1
JOIN sandbox.load_dc.zhv_perevods a2 ON a1.whs_di=a2.whs_di AND ((a1.week>=a2.week and year = 2023) or year = 2024) AND (a1.di = a2.di_old or a1.di = di_new) and a1.week>@Week

insert into result
SELECT*FROM ##perevods
--------------------------------------------------------

------------------�������� ������� �������--------------
SELECT whs_di, sum(box)
FROM result where di = 28508 and ((week >= 30 and year = 2023) or year = 2024) and box!=0
group by whs_di
order by 2 desc

SELECT distinct whs_di
FROM result where di = 28508 and ((week >= 30 and year = 2023) or year = 2024)

update result set box=0 where di = 28508 and ((week >= 30 and year = 2023) or year = 2024)

-----------------�������� ������� ���������----------------
SELECT max(box)
FROM result where di = 27089 and ((week >= 14 and year = 2023) or year = 2024) and delivery_type = '��'

SELECT distinct whs_di
FROM result where di = 27089 and ((week >= 14 and year = 2023) or year = 2024) and delivery_type = '��'

update result set box=0 where di = 27089 and ((week >= 14 and year = 2023) or year = 2024) and delivery_type = '��'
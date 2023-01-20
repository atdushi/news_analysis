with 1 as dataset
select 
	category,
	count(*) "Всего новостей",
	countIf(title, site='LENTA.RU') "LENTA.RU",
	countIf(title, site='ВЕДОМОСТИ') "ВЕДОМОСТИ",
	countIf(title, site='TASS') "TASS",
	countIf(title, site='Фонтанка.ру') "Фонтанка.ру"
from news n
where (toDate(pub_date) = yesterday() and dataset=2) or dataset=1
group by category
order by category
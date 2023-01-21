with 1 as dataset
select
    row_number() over() id, -- eyewash
	category,
	count(*) "Всего новостей",
	countIf(title, site='LENTA.RU') "LENTA.RU",
	countIf(title, site='ВЕДОМОСТИ') "ВЕДОМОСТИ",
	countIf(title, site='TASS') "TASS",
	countIf(title, site='Фонтанка.ру') "Фонтанка.ру"
from news n
where (toDate(pub_date) = yesterday() and dataset=2) or dataset=1
group by category
order by category;

with 
	aux1 as
	(
		select 
			toDate(pub_date) pd, 
			category, 
			site, 
			count(*) news_count
		from news
		group by toDate(pub_date), category, site
		order by toDate(pub_date), category, site
	),
	aux2 as
	(
		select 
			category, 
			avg(news_count) avg_news_count,
			max(news_count) max_news_count
		from aux1
		group by category
	)
select distinct
	a1.category,
	a1.pd max_news_date,
	a2.max_news_count
from aux1 a1
join aux2 a2 on a1.category = a2.category
where a1.news_count=a2.max_news_count
order by a1.category;
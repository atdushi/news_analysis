-- datasets 1, 2
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

-- dataset 3
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

-- dataset 4
with 
	d as
	(
		select category ,days as day_of_week
		from
		(
			select category, ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'] as days
			from news n 
			group by category
		)
		array join days
	),
	g as
	(
		select category, day_of_week, count(*) news_count
		from news
		group by category, day_of_week
	),
	days AS 
	(
		select 'Mon' as day_of_week, 1 as ord
		union all
		select 'Tue' as day_of_week, 2 as ord
		union all
		select 'Wed' as day_of_week, 3 as ord
		union all
		select 'Thu' as day_of_week, 4 as ord
		union all
		select 'Fri' as day_of_week, 5 as ord
		union all
		select 'Sat' as day_of_week, 6 as ord
		union all
		select 'Sun' as day_of_week, 7 as ord
	)
select d.category, d.day_of_week, g.news_count
from d
join days on d.day_of_week=days.day_of_week
left join g on d.category = g.category and d.day_of_week = g.day_of_week
order by d.category, days.ord;

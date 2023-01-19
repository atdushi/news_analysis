select category , count(*) news_count
from news n 
group by category
order by category
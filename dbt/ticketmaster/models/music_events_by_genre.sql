select
    genre,
    count(event_tid) as events
from 
    ticketmaster.events
where
    segment = 'Music'
group by
    genre
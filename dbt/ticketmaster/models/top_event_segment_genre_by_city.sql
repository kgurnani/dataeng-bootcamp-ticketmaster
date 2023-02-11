with group_with_row_number as (
    select
        venue_city_name,
        venue_state_name,
        venue_country_name,
        segment,
        genre,
        count(event_tid) as events,
        row_number() OVER (PARTITION BY venue_city_name, venue_state_name, venue_country_name ORDER BY count(event_tid) DESC) AS row_num
    from ticketmaster.events
    where 
        (segment != "Undefined" and segment != "Miscellaneous" and segment != "NONE") and
        (genre != "Undefined" and genre != "Miscellaneous" and genre != "NONE" and genre != "Other")
    group by
        venue_city_name,
        venue_state_name,
        venue_country_name,
        segment,
        genre
)
select * from group_with_row_number where row_num = 1
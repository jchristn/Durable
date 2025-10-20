SELECT 
    CAST(CAST('2.14:30:45' AS TIME) AS SIGNED) as time_as_int,
    TIME_TO_SEC('2.14:30:45') as time_to_sec;

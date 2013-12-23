data = LOAD '../text_data/u.data' USING PigStorage('\t') AS (id:int, movie:int, rating:int, timestamp:long)

notimestamp = FOREACH data GENERATE id, movie, rating;

dump notimestamp;


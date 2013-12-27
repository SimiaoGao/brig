SET DEFAULT_PARALLEL $num;

DEFINE DUPLICATE(in) RETURNS out
{
        $out = FOREACH $in GENERATE *;
};

data = LOAD '$input' USING PigStorage('\t') AS (id:int, movie:int, rating:int, timestamp:long);        
  
nots = FOREACH data GENERATE id, movie, rating;
not2 = DUPLICATE(nots);
joid = JOIN nots BY id, not2 BY id;
noid = FOREACH joid GENERATE $1 as movie1, $4 as movie2, $2 as rating1, $5 as rating2;
noid = FILTER noid BY movie1 != movie2; 
grpd = GROUP noid BY (movie1, movie2); 
rslt = FOREACH grpd GENERATE group, noid.(rating1, rating2); 

STORE rslt INTO '$output' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('c:rating');


-- Movie Data Analytics

-- Number of movies released between 1950 and 1960.

data = LOAD '/user/edureka/aprl_25th_morng/movies_project/input/dataset.txt' using PigStorage(',') as (d1 : int, d2 : chararray, d3 : int, d4 : float, d5 : int);
dump data;
reduced = FILTER data BY d3 > 1950 AND d3 < 1960 ; 
dump reduced; 
list = GROUP reduced ALL;
dump list;
count = FOREACH list GENERATE COUNT(reduced);
dump count;
STORE count INTO '/user/edureka/aprl_25th_morng/movies_project/op_1';


-- Number of movies having rating more than 4.

rating = FILTER data BY d4 > 4.0 ;
dump rating;
list2 = GROUP rating ALL;
dump list2;
count2 = FOREACH list2 GENERATE COUNT(rating);
dump count2;
STORE count2 INTO '/user/edureka/aprl_25th_morng/movies_project/op_2';

-- Movies whose rating are between 3 and 4.

ratng_btwn = FILTER data BY d4 > 3.0 AND d4 < 4.0 ;
dump ratng_btwn;
list3 = FOREACH ratng_btwn GENERATE d2, d4 ;
dump list3;
STORE list3 INTO '/user/edureka/aprl_25th_morng/movies_project/op_3';

-- Number of movies with duration more than 2 hours.

duration = FILTER data BY d5 > 7200 ;
dump duration ;
list4 = GROUP duration ALL ;
dump list4 ;
count3 = FOREACH list4 GENERATE COUNT(duration);
dump count3 ;
STORE count3 INTO '/user/edureka/aprl_25th_morng/movies_project/op_4';

-- List of years and number of movies released each year.

G = GROUP data BY d3 ;
dump G ;
list5 = FOREACH G GENERATE group , COUNT(data) ;
dump list5 ;
STORE list5 INTO '/user/edureka/aprl_25th_morng/movies_project/op_5';

-- Total number of movies in the dataset.

list6 = GROUP data ALL;
dump list6;
count4 = FOREACH list6 GENERATE COUNT(data);
dump count4;
STORE count4 INTO '/user/edureka/aprl_25th_morng/movies_project/op_6';



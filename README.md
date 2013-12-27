Blockbuster Recommender Ultimate Guru
====

A simple MapReduce program that correlates similar movies together.

Different implementations can be found at src/edu/rutgers/ece451:

1.  *Recommender.java* a non-mapreduced implementation of movie recommender

2.  *MovieRecommenderHadoop.java* a hadoop-powered implementation of movie recommender

3.  *Brug.java* part of the polyglot implementation of movie recommender that handles correlation calculation using Hadoop.


Non-Mapreduce Implementation
---
To run *Recommender.java*, 


MapReduced Implementation
---
To run *MovieRecommenderHadoop.java*,


Polyglot Implementation
---
Lastly, *Brug.java* is a part of the polyglot version of the program. You need to complete the previous steps in order to run it. In detail:

1.  Set up Pig and HBase, under HBase Shell, type commands: 

        create 'pairs', 'c'

        create 'top', 'c'

    Verify that you've set up the table by typing:

        list

2.  Run the pig script, which is located under scripts folder. Execute the command:

        pig -x local scripts/udata.pig -param input=<input file location> -param output=<output file location> -param num=<number of parallel jobs>

    for example to run it on local file u.data under text_data directory without parallelism and output to *pairs* HBase table, the command should be:

        pig -x local scripts/udata.pig text_data/u.data -param input=text_data/u.data -param num=1 -param output=hbase://pairs

3.  Run Hadoop Correlation program (*Brug.java*). First compile it using maven by typing:

        mvn clean install

    A jar file named *brug-0.0.1-SNAPSHOT-jar-with-dependencies.jar* should be generated under target directory. Execute

        hadoop jar brug-0.0.1-SNAPSHOT-jar-with-dependencies.jar Brug

    will process the data under `hbase://pairs` and output result to `hbase://top`



First, copy the "input" folder in the home/hadoop folder.

In the terminal run: 
hdfs dfs -mkdir -p ~/input/
hdfs dfs -put ~/input ~/input
(This will copy input files in hdfs)

Using terminal, cd into the "jars" folder.

WORDCOUNT:
Run:
hadoop jar wc.jar WordCount ~/input/input/WordCount/ ~/wc
Output:
hdfs dfs -get ~/wc

R Notebook for wordcloud is in the "visualization" folder.

PAIRS:
Run:
hadoop jar pairs.jar Pairs ~/input/input/PS/ ~/pairs
Output:
hdfs dfs -get ~/pairs

STRIPES:
Run:
hadoop jar stripes.jar Stripes ~/input/input/PS/ ~/stripes
Output:
hdfs dfs -get ~/stripes

FEATURED ACTIVITY 1:
Run:
hadoop jar fa1.jar FA1 ~/input/input/FA1/ ~/fa1
Output:
hdfs dfs -get ~/fa1

FEATURED ACTIVITY 2A:
Run:
hadoop jar fa2a.jar FA2A ~/input/input/FA2/ ~/fa2a
Output:
hdfs dfs -get ~/fa2a

FEATURED ACTIVITY 2B:
Run:
hadoop jar fa2b.jar FA2B ~/input/input/FA2/ ~/fa2b
Output:
hdfs dfs -get ~/fa2b

Performance Graph is in the "visualization" folder.

(OUTPUT FOLDERS WILL APPEAR IN "jars" folder.)

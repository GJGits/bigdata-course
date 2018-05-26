# TODO: use scp instead of filezilla to upload files to gateway!

# Remove folders of the previous run
hdfs dfs -rm -r hadoop-outputs/ex6

# Run application
hadoop jar ../jars/ex6.jar it.polito.gjcode.hadoop.ex3.Driver data/ex3  hadoop-outputs/ex6
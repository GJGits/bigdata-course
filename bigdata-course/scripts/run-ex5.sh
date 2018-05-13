# TODO: use scp instead of filezilla to upload files to gateway!

# Remove folders of the previous run
hdfs dfs -rm -r hadoop-outputs/ex5

# Run application
hadoop jar ../jars/ex5.jar it.polito.gjcode.hadoop.ex5.Driver data/ex3  hadoop-outputs/ex5 
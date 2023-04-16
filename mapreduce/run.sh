export MR_OUTPUT=/user/ikorchagin/output-data

hadoop fs -rm -r $MR_OUTPUT


hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
-Dmapred.job.name='Homework practice2 mapreduce job' \
-Dmapred.reduce.tasks=1 \
-file /tmp/mapreduce/mapper.py -mapper /tmp/mapreduce/mapper.py \
-file /tmp/mapreduce/reducer.py -reducer /tmp/mapreduce/reducer.py \
-input /user/root/2020/ -output $MR_OUTPUT

# -Dmapred.reduce.tasks=1 \
#-Dmapreduce.input.lineinputformat.linespermap=1000 \
#-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \



#-Dmapred.job.name='Simple streaming job' \
#-Dmapred.reduce.tasks=1 -Dmapreduce.input.lineinputformat.linespermap=1000 \
#-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \


# -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat -Dmapreduce.input.lineinputformat.linespermap=1000 \

## s3
# -Dfs.s3a.endpoint=s3.amazonaws.com -Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider


#export MR_OUTPUT=/user/ikorchagin/taxi-output
#
#hadoop fs -rm -r taxi-output
#
#hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
#-Dmapred.job.name='Taxi streaming job' \
#-Dmapred.reduce.tasks=10 \
#-Dmapreduce.map.memory.mb=1024 \
#-file /tmp/mapreduce/mapper.py -mapper /tmp/mapreduce/mapper.py \
#-file /tmp/mapreduce/reducer.py -reducer /tmp/mapreduce/reducer.py \
#-input /user/root/2020/yellow_tripdata_2020-1*.csv  -output $MR_OUTPUT

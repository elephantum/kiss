SET mapred.output.compress=true;
SET hive.exec.compress.output=true; 
SET io.seqfile.compression.type=BLOCK;
 
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;


create external table kiss_raw (
    json_data string
)
location 's3://enter-kiss-test/revisions/' ;

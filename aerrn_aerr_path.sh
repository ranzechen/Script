#!/bin/bash
#author:ranzechen
#function:依次获取hdfs上/data/shoudan/日期/每个流水目录中的文件----规则:先取流水中以AERRN结尾文件，没有的话再取以AERR结尾的文件

time=`date -d "2 day ago" +"%Y%m%d"`
hdfs_path="hdfs://100.1.1.32:9200"

dir_path=`hadoop fs -du /data/shoudan/$time | awk '{print$2}' | grep "shoudan"`
for i in $dir_path
do
	file_path_aerrn=`hadoop fs -du $i | awk '{print$2}' | grep "AERRN$"`
        if [ $? -ne 0 ];then
		file_path_aerr=`hadoop fs -du $i | awk '{print$2}' | grep  "AERR$"`
		if [ $? -ne 0 ];then
	              echo "not find *AERRN *AERR path in $i"
                fi
	fi
	
	if [ -n "$file_path_aerrn" ];then
		echo ">>>>>Spark aerrn app"
		for aerrn in $file_path_aerrn
		do
		    size=`hadoop fs -du $aerrn | awk '{print$1}' | grep "[0-9]"`
		    if [ $size -ne 0 ];then
			    echo ">>>>>data source:"$hdfs_path$aerrn $time
			fi
		done
	fi

	if [ -n "$file_path_aerr" ];then
		echo ">>>>>Spark aerr app"
		for aerr in $file_path_aerr
		do
		    size=`hadoop fs -du $aerr | awk '{print$1}' | grep "[0-9]"`
		    if [ $size -ne 0 ];then
			    echo ">>>>>data source:"$hdfs_path$aerr $time
			fi
		done
	fi
done

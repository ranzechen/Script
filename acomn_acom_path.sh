#!/bin/bash
#author:ranzechen
#function:依次获取hdfs上/data/shoudan/日期/每个流水目录中的文件----规则:先取流水中以ACOMN结尾文件，没有的话再取以ACOM结尾的文件，在没有的话取以ACOMA结尾的文件

time=`date -d "2 day ago" +"%Y%m%d"`
hdfs_path="hdfs://100.1.1.32:9200"

dir_path=`hadoop fs -du /data/shoudan/$time | awk '{print$2}' | grep "shoudan"`
for i in $dir_path
do
	file_path_acomn=`hadoop fs -du $i | awk '{print$2}' | grep "ACOMN$"`
        if [ $? -ne 0 ];then
		file_path_acom=`hadoop fs -du $i | awk '{print$2}' | grep  "ACOM$"`
		if [ $? -ne 0 ];then
			#file_path_acoma=`hadoop fs -du $i | awk '{print$2}' | grep  "ACOMA$"`
                        #if [ $? -ne 0 ];then
	                        echo "not find *ACOMN *ACOM path in $i"
			#fi
                fi
	fi

	if [ -n "$file_path_acomn" ];then
		echo ">>>>>Spark acomn app"
		for acomn in $file_path_acomn
		do
		    size=`hadoop fs -du $acomn | awk '{print$1}' | grep "[0-9]"`
		    if [ $size -ne 0 ];then
			    echo ">>>>>data source:"$hdfs_path$acomn $time
			fi
		done
	fi

	if [ -n "$file_path_acom" ];then
		echo ">>>>>Spark acom app"
		for acom in $file_path_acom
		do
		    size=`hadoop fs -du $acom | awk '{print$1}' | grep "[0-9]"`
		    if [ $size -ne 0 ];then
			    echo ">>>>>data source:"$hdfs_path$acom $time
			fi
		done
	fi
done

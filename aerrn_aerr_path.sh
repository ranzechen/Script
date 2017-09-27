#!/bin/bash
#author:ranzechen
#function:依次获取hdfs上/data/shoudan/日期/每个流水目录中的文件----规则:先取流水中以AERRN结尾文件，没有的话再取以AERR结尾的文件

if [ $# != 1 ] ; then 
	time=`date -d "2 day ago" +"%Y%m%d"`
else
	time=$1
fi

hdfs_path="hdfs://100.1.1.39:8020"

dir_path=`hadoop fs -du /data/shoudan/$time | awk '{print$2}' | grep "shoudan"`
for i in $dir_path
do
	echo "reading dirname is :"$i
	file_path_aerrn=`hadoop fs -du $i | awk '{if($1 != 0)print $2}' | grep "AERRN$" | grep "IND"`
        if [ $? -eq 0 ];then
		for aerrn in $file_path_aerrn
		do
			if [ -n "$aerrn" ];then
				jigouhao=`echo $aerrn | awk -F '/' '{print$5}'`
				filename=`echo $aerrn | awk -F '/' '{print$6}'`
				echo ">>>>>Spark aerrn app"
				echo ">>>>>data source:"$hdfs_path$aerrn $time $jigouhao $filename
			fi
		done
	else
		file_path_aerr=`hadoop fs -du $i | awk '{if($1 != 0)print $2}' | grep  "AERR$" | grep "IND"`
		if [ $? -eq 0 ];then
			for aerr in $file_path_aerr
			do
				if [ -n "$file_path_aerr" ];then
					jigouhao=`echo $aerr | awk -F '/' '{print$5}'`
					filename=`echo $aerr | awk -F '/' '{print$6}'`
					echo ">>>>>Spark aerr app"
					echo ">>>>>data source:"$hdfs_path$aerr $time $jigouhao $filename
				fi
			done
		else
                        echo "not find *AERRN *AERR path in $i"
		fi
	fi
done





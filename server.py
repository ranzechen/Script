#!coding=UTF-8
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from subprocess import Popen, PIPE
import time
import io, shutil, urllib, os
import subprocess
import logging
import logging.handlers
import json
import sys

class MyHttpHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global tablename
        global es_type
        if '?' in self.path:
            self.queryString = urllib.parse.unquote(self.path.split('?', 1)[1])
            params = urllib.parse.parse_qs(self.queryString)
            # 查询es中某个type下的笔数和总金额
            if "es_type" in params:
                es_type = str(bytes(params["es_type"][0], 'iso-8859-1'), 'GBK')         
                es_type = params["es_type"][0]
                type_result = os.popen("curl -XGET http://DXIDC7GBBIGDATAN34:9200/_search -d'{" + "\"from\"" + ":0," + "\"size\"" + ":0," + "\"query\"" + ":{" + "\"bool\"" + ":{" + "\"must\"" + ":[{" + "\"match\"" + ":{" + "\"_type\"" + ":" + "\"" + es_type + "\"" + "}},{" + "\"match\"" + ":{" + "\"sett_flag\"" + ":" + "\"1\"" + "}}]}}," + "\"aggs\"" + ":{" + "\"intraday_return\"" + ":{" + "\"sum\"" + ":{" + "\"field\"" + ":" + "\"tftxmony\"" + "}}}}'").read().strip('\n')
                type_json = json.loads(type_result)
                type_sum = type_json['hits']['total']
                type_tftxmony = type_json['aggregations']['intraday_return']['value']
                type_str = "0000#DATE=" + es_type + ",CNT=" + str(type_sum) + ",MONEY=" + str('%.2f' % (type_tftxmony))
                enc = "UTF-8"
                in_encoded = ''.join(type_str).encode(enc)
                f = io.BytesIO()
                f.write(in_encoded)
                f.seek(0)
                self.send_response(200)
                self.send_header("Content-type", "text/html;charset=%s" % enc)
                self.send_header("Content-Length", str(len(in_encoded)))
                self.end_headers()
                shutil.copyfileobj(f, self.wfile)
                os.system("echo" + '' + " ..........................................................................")
            elif "ygbx_es_type" in params:
                ygbx_es_type = str(bytes(params["ygbx_es_type"][0], 'iso-8859-1'), 'GBK')
                ygbx_es_type = params["ygbx_es_type"][0]
                type_result = os.popen("curl -XGET 100.1.1.43:9200/_search -d'{" + "\"from\"" + ":0," + "\"size\"" + ":0," + "\"query\"" + ":{" + "\"bool\"" + ":{" + "\"must\"" + ":[{" + "\"match\"" + ":{" + "\"_type\"" + ":" + "\"" + ygbx_es_type + "\"" + "}},{" + "\"match\"" + ":{" + "\"sett_flag\"" + ":" + "\"1\"" + "}}]}}," + "\"aggs\"" + ":{" + "\"intraday_return\"" + ":{" + "\"sum\"" + ":{" + "\"field\"" + ":" + "\"tftxmony\"" + "}}}}'").read().strip('\n')
                type_json = json.loads(type_result)
                type_sum = type_json['hits']['total']
                type_tftxmony = type_json['aggregations']['intraday_return']['value']
                type_str = "0000#DATE=" + ygbx_es_type + ",CNT=" + str(type_sum) + ",MONEY=" + str('%.2f' % (type_tftxmony))
                enc = "UTF-8"
                in_encoded = ''.join(type_str).encode(enc)
                f = io.BytesIO()
                f.write(in_encoded)
                f.seek(0)
                self.send_response(200)
                self.send_header("Content-type", "text/html;charset=%s" % enc)
                self.send_header("Content-Length", str(len(in_encoded)))
                self.end_headers()
                shutil.copyfileobj(f, self.wfile)
                os.system("echo" + '' + " ..........................................................................")    
            #根据表名生成所有商户报表模块
            elif "report_table" in params:
                os.system("echo"+''+" ..........................................................................")
                print(">>>>>生成所有商户报表")
                report_table=str(bytes(params["report_table"][0],'iso-8859-1'),'GBK')
                report_table=params["report_table"][0] if "report_table" in params else None
                r_sqoop=os.popen("sqoop list-tables --connect jdbc:oracle:thin:@100.1.1.203:1521:ORCL --username JFZXMG --password  WiKtZIoeqp4ktOALEEoZQpEnLSx7Il | grep -w "+report_table,'w')
                r_sqoop_rc=r_sqoop.close()
                print(r_sqoop_rc)
                if r_sqoop_rc == None or r_sqoop_rc % 256:
                    r_targetdir=os.popen("echo " + report_table + "| awk -F '_' '{print $3}' | cut -b 1-4").read().strip('\n')
                    os.system("echo"+''+" 表名:"+''+report_table)
                    os.system("echo"+''+" 目录:/data/ybs_sett/"+''+r_targetdir)
                    hdfspath=os.popen("hadoop fs -ls /data/ybs_sett/" + r_targetdir+"| grep -w "+report_table+"|awk '{print $8}'").read().strip('\n')
                    if hdfspath != "":
                        os.system("sed -i /settFilePath/d /home/bigdata/ranzechen/conf.properties")
                        os.system("sed -i /tfmccode/d /home/bigdata/ranzechen/conf.properties")
                        os.system("echo tfmccode=all >> /home/bigdata/ranzechen/conf.properties")
                        os.system("sed -i /datafrom/d /home/bigdata/ranzechen/conf.properties")
                        os.system("echo datafrom=all >> /home/bigdata/ranzechen/conf.properties")
                        os.system("echo settFilePath=hdfs://DXIDC7GBBIGDATAN39:8020"+''+str(hdfspath)+''+">>/home/bigdata/ranzechen/conf.properties")
                        sttime=datetime.datetime.now()
                        hdfs_spark=os.system("/usr/local/spark/bin/spark-submit --master spark://DXIDC7GBBIGDATAN39:7077 --class com.spark.report.main.FirstSparkApp --executor-memory 10g --driver-memory  10g --total-executor-cores 20 /home/bigdata/ranzechen/report.jar &> /data/REPORT_SPARK_LOG/"+''+report_table+".txt")
                        restime=datetime.datetime.now()
                        r_spark_time=(restime - sttime).seconds
                        os.system("echo"+''+" make report spark执行时间:"+''+str(r_spark_time)+"s")
                        if hdfs_spark == 0:
                            hdfs_spark_info="spark make report success！！！"
                            os.system("/home/bigdata/ranzechen/test.sh > /data/send_report.log")
                            os.system("sh /home/bigdata/ranzechen/ftp.sh")
                        else:
                            hdfs_spark_info="spark make report fail！！！"
                            os.system("echo"+''+" spark执行结果:"+''+str(hdfs_spark_info))
                            rr_table_str="0000#make report is : "+report_table+">>>"+"OK..."
                            enc="UTF-8"
                            rrencoded = ''.join(rr_table_str).encode(enc)
                            f = io.BytesIO()
                            f.write(rrencoded)
                            f.seek(0)
                            self.send_response(200)
                            self.send_header("Content-type", "text/html;charset=%s" % enc)
                            self.send_header("Content-Length", str(len(rrencoded)))
                            self.end_headers()
                            shutil.copyfileobj(f,self.wfile)
                            os.system("echo"+''+" ..........................................................................")
                    else:
                        r_starttime=datetime.datetime.now()
                        r_sqoop_status=os.system("sqoop import --append --connect jdbc:oracle:thin:@100.1.1.203:1521:ORCL --username JFZXMG --password WiKtZIoeqp4ktOALEEoZQpEnLSx7Il  --fields-terminated-by \"衚\"  --hive-drop-import-delims --lines-terminated-by \"\n\" --target-dir /data/ybs_sett/"+''+r_targetdir+" --table "+''+report_table+" --verbose -m 1 &> /data/REPORT_SQOOP_LOG/"+''+report_table+".txt")
                        if r_sqoop_status == 0:
                            
                            r_sqoop_info="sqoop 执行成功！！！"
                        else:
                            r_sqoop_info="sqoop 执行失败！！！"
                            os.system("echo"+''+" sqoop执行结果:"+''+str(r_sqoop_info))
                            r_endtime = datetime.datetime.now()
                        #获取sqoop执行的时间差,秒
                        r_time=(r_endtime - r_starttime).seconds
                        os.system("echo"+''+" sqoop执行时间:"+''+str(r_time)+"s")
                        r_part=os.popen("grep repartitioned /data/REPORT_SQOOP_LOG/"+''+report_table+".txt | awk '{print $9}'").read().strip('\n')
                        os.system("hadoop fs -mv /data/ybs_sett/"+''+r_targetdir+"/"+''+r_part+" /data/ybs_sett/"+''+r_targetdir+"/"+''+report_table)
                        r_path=os.popen("hadoop fs -ls /data/ybs_sett/"+''+r_targetdir+"/"+''+report_table+"|awk '{print $8}'").read().strip('\n')
                        os.system("echo"+''+" 路径为:"+''+str(r_path))            
                        os.system("sed -i /settFilePath/d /home/bigdata/ranzechen/conf.properties")
                        os.system("sed -i /tfmccode/d /home/bigdata/ranzechen/conf.properties")
                        os.system("echo tfmccode = all >> /home/bigdata/ranzechen/conf.properties")
                        os.system("sed -i /datafrom/d /home/bigdata/ranzechen/conf.properties")
                        os.system("echo datafrom=all >> /home/bigdata/ranzechen/conf.properties")
                        os.system("echo settFilePath = hdfs://DXIDC7GBBIGDATAN39:8020"+''+str(r_path)+''+">>/home/bigdata/ranzechen/conf.properties")
                        r_start_spark_time=datetime.datetime.now()
                        r_spark=os.system("/usr/local/spark/bin/spark-submit --master spark://DXIDC7GBBIGDATAN39:7077 --class com.spark.report.main.FirstSparkApp --executor-memory 10g --driver-memory  10g --total-executor-cores 20 /home/bigdata/ranzechen/report.jar &> /data/REPORT_SPARK_LOG/"+''+report_table+".txt")
                        r_end_spark_time=datetime.datetime.now()
                        r_spark_time=(r_end_spark_time - r_start_spark_time).seconds
                        os.system("echo"+''+" make report spark执行时间:"+''+str(r_spark_time)+"s") 
                        if r_spark == 0:
                            r_spark_info="spark make report success！！！"
                            os.system("/home/bigdata/ranzechen/test.sh > /data/send_report.log")
                            os.system("sh /home/bigdata/ranzechen/ftp.sh")    
                        else:
                            r_spark_info="spark make report fail！！！"
                        os.system("echo"+''+" spark执行结果:"+''+str(r_spark_info))
                        r_table_str="0000#make report is : "+report_table+">>>"+"OK..."
                        enc="UTF-8"
                        rencoded = ''.join(r_table_str).encode(enc)
                        f = io.BytesIO()
                        f.write(rencoded)
                        f.seek(0)
                        self.send_response(200)
                        self.send_header("Content-type", "text/html;charset=%s" % enc)
                        self.send_header("Content-Length", str(len(rencoded)))
                        self.end_headers()
                        shutil.copyfileobj(f,self.wfile)
                        os.system("echo"+''+" ..........................................................................")
                else:
                    no_table_str="0002#the table is not exist!!!"
                    enc="UTF-8"
                    norencoded = ''.join(no_table_str).encode(enc)
                    f = io.BytesIO()
                    f.write(norencoded)
                    f.seek(0)
                    self.send_response(200)
                    self.send_header("Content-type", "text/html;charset=%s" % enc)
                    self.send_header("Content-Length", str(len(norencoded)))
                    self.end_headers()
                    shutil.copyfileobj(f,self.wfile)    
#############根据表名生成某些商户的报表
            elif "report_table_tfmccode" in params:
                print(">>>>>生成某些商户的报表")
                report_table_tfmccode=str(bytes(params["report_table_tfmccode"][0],'iso-8859-1'),'GBK')
                report_table_tfmccode=params["report_table_tfmccode"][0] if "report_table_tfmccode" in params else None
                print(report_table_tfmccode)
                stddatafrom=report_table_tfmccode.split("/")[1].split("=")[1]
                if str(stddatafrom) == "jiaofei" or str(stddatafrom) == "bank" or str(stddatafrom) == "zhilian" or str(stddatafrom) == "all" :
                    stdtfmccode=report_table_tfmccode.split("/")[2].split("=")[1]
                    numtfmccode=os.popen("expr length "+stdtfmccode).read().strip('\n')
                    if int(numtfmccode) == 18 or int(numtfmccode) == 8 or str(stdtfmccode) == "all":
                        rtt_table=report_table_tfmccode.split("/")[0]
                        #执行list-tables判断是否存在这张表
                        rtt_sqoop=os.system("sqoop list-tables --connect jdbc:oracle:thin:@100.1.1.203:1521:ORCL --username JFZXMG --password  WiKtZIoeqp4ktOALEEoZQpEnLSx7Il | grep -w "+rtt_table)
                        if rtt_sqoop == 0:
                            rtt_targetdir=os.popen("echo " + rtt_table + "| awk -F '_' '{print $3}' | cut -b 1-4").read().strip('\n')
                            os.system("echo"+''+" ..........................................................................")
                            os.system("echo"+''+" 表名:"+''+rtt_table)
                            os.system("echo"+''+" 目录:/data/ybs_sett/"+''+rtt_targetdir)
                            rtt_hdfspath=os.popen("hadoop fs -ls /data/ybs_sett/" + rtt_targetdir+"| grep -w "+rtt_table+"|awk '{print $8}'").read().strip('\n')
                            if rtt_hdfspath != "":
                                os.system("echo"+''+" 路径为:"+''+str(rtt_hdfspath))
                                os.system("sed -i /settFilePath/d /home/bigdata/ranzechen/ReportBankConf.properties")
                                os.system("echo settFilePath=hdfs://DXIDC7GBBIGDATAN39:8020"+''+str(rtt_hdfspath)+''+">>/home/bigdata/ranzechen/ReportBankConf.properties")
                                os.system("sed -i /datafrom/d /home/bigdata/ranzechen/ReportBankConf.properties")
                                datafrom=os.popen("echo "+report_table_tfmccode +"| awk -F '/' '{print $2}'").read().strip('\n')
                                flag=os.popen("echo " +datafrom +"|awk -F '=' '{print $2}'").read().strip('\n')
                                if str(flag) == "bank":
                                    os.system("echo "+''+str(datafrom)+''+">>/home/bigdata/ranzechen/ReportBankConf.properties")
                                    bankStatus=os.system("/usr/local/spark/bin/spark-submit --master spark://DXIDC7GBBIGDATAN39:7077 --class data.spark.batch.bankreport.app.ReportBankApp --executor-memory 10g --driver-memory 10g --total-executor-cores 20 /home/bigdata/ranzechen/reportBank.jar &> /data/REPORT_SPARK_LOG/"+''+rtt_table+".txt")
                                    if bankStatus == 0:
                                        bankinfo="spark make report success！！！"
                                        os.system("/home/bigdata/ranzechen/test.sh > /data/send_report.log")
                                        os.system("sh /home/bigdata/ranzechen/ftp.sh")
                                        bank_str="0000#make report is: "+rtt_table+">>>"+"success..."
                                        enc="UTF-8"
                                        bank_encoded = ''.join(bank_str).encode(enc)
                                        f = io.BytesIO()
                                        f.write(bank_encoded)
                                        f.seek(0)
                                        self.send_response(200)
                                        self.send_header("Content-type", "text/html;charset=%s" % enc)
                                        self.send_header("Content-Length", str(len(bank_encoded)))
                                        self.end_headers()
                                        shutil.copyfileobj(f,self.wfile)
                                    else:
                                        bankinfo="spark make report fail！！！"
                                        os.system("echo"+''+" spark执行结果:"+''+str(bankinfo))
                                        bank_str="0000#make report is: "+rtt_table+">>>"+"fail..."
                                        enc="UTF-8"
                                        bank_encoded = ''.join(bank_str).encode(enc)
                                        f = io.BytesIO()
                                        f.write(bank_encoded)
                                        f.seek(0)
                                        self.send_response(200)
                                        self.send_header("Content-type", "text/html;charset=%s" % enc)
                                        self.send_header("Content-Length", str(len(bank_encoded)))
                                        self.end_headers()
                                        shutil.copyfileobj(f,self.wfile)
                                else:
                                    os.system("echo "+''+str(datafrom)+''+">>/home/bigdata/ranzechen/conf.properties")
                                    os.system("sed -i /tfmccode/d /home/bigdata/ranzechen/conf.properties")
                                    tfmccode=os.popen("echo "+report_table_tfmccode +"| awk -F '/' '{print $3}'").read().strip('\n')
                                    os.system("echo "+''+str(tfmccode)+''+">> /home/bigdata/ranzechen/conf.properties")
                                    rtt_spark=os.system("/usr/local/spark/bin/spark-submit --master spark://DXIDC7GBBIGDATAN39:7077 --class com.spark.report.main.FirstSparkApp --executor-memory 10g --driver-memory  10g --total-executor-cores 20 /home/bigdata/ranzechen/report.jar &> /data/REPORT_SPARK_LOG/"+''+rtt_table+".txt")
                                    if rtt_spark == 0:
                                        rtt_spark_info="spark make report success！！！"
                                        os.system("/home/bigdata/ranzechen/test.sh > /data/send_report.log")
                                        os.system("sh /home/bigdata/ranzechen/ftp.sh")
                                    else:
                                        rtt_spark_info="spark make report fail！！！"
                                        os.system("echo"+''+" spark执行结果:"+''+str(rtt_spark_info))
                                        rtt_status_str1="0000#make report is: "+rtt_table+">>>"+"OK..."
                                        enc="UTF-8"
                                        rtt_encoded1 = ''.join(rtt_status_str1).encode(enc)
                                        f = io.BytesIO()
                                        f.write(rtt_encoded1)
                                        f.seek(0)
                                        self.send_response(200)
                                        self.send_header("Content-type", "text/html;charset=%s" % enc)
                                        self.send_header("Content-Length", str(len(rtt_encoded1)))
                                        self.end_headers()
                                        shutil.copyfileobj(f,self.wfile)
                                        os.system("echo"+''+" ..........................................................................")        
                            else:
                                rtt_starttime = datetime.datetime.now()
                                rtt_sqoop_status=os.system("sqoop import --append --connect jdbc:oracle:thin:@100.1.1.203:1521:ORCL --username JFZXMG --password WiKtZIoeqp4ktOALEEoZQpEnLSx7Il  --fields-terminated-by \"衚\"  --hive-drop-import-delims --lines-terminated-by \"\n\" --target-dir /data/ybs_sett/"+''+rtt_targetdir+" --table "+''+rtt_table+" --verbose -m 1 &> /data/REPORT_SQOOP_LOG/"+''+rtt_table+".txt")
                                if rtt_sqoop_status == 0:
                                    rtt_sqoop_info="sqoop 执行成功！！！"
                                else:
                                    rtt_sqoop_info="sqoop 执行失败！！！"
                                os.system("echo"+''+" sqoop执行结果:"+''+str(rtt_sqoop_info))
                                rtt_endtime = datetime.datetime.now()
                                #获取sqoop执行的时间差,秒
                                rtt_time=(rtt_endtime - rtt_starttime).seconds
                                os.system("echo"+''+" sqoop执行时间:"+''+str(rtt_time)+"s")
                                rtt_part=os.popen("grep repartitioned /data/REPORT_SQOOP_LOG/"+''+rtt_table+".txt | awk '{print $9}'").read().strip('\n')
                                os.system("hadoop fs -mv /data/ybs_sett/"+''+rtt_targetdir+"/"+''+rtt_part+" /data/ybs_sett/"+''+rtt_targetdir+"/"+''+rtt_table)
                                rtt_path=os.popen("hadoop fs -ls /data/ybs_sett/" + rtt_targetdir + "/"+rtt_table+"|awk '{print $8}'").read().strip('\n')
                                os.system("echo"+''+" 路径为:"+''+str(rtt_path))
                                os.system("sed -i /settFilePath/d /home/bigdata/ranzechen/ReportBankConf.properties")
                                os.system("echo settFilePath = hdfs://DXIDC7GBBIGDATAN39:8020"+''+str(rtt_path)+''+">>/home/bigdata/ranzechen/ReportBankConf.properties")
                                os.system("sed -i /datafrom/d /home/bigdata/ranzechen/ReportBankConf.properties")
                                datafrom=os.popen("echo "+report_table_tfmccode +"| awk -F '/' '{print $2}'").read().strip('\n')
                                flag=os.popen("echo " +datafrom +"|awk -F '=' '{print $2}'").read().strip('\n')
                                if str(flag) == "bank":
                                    os.system("echo "+''+str(datafrom)+''+">> /home/bigdata/ranzechen/ReportBankConf.properties")
                                    bankStatus=os.system("/usr/local/spark/bin/spark-submit --master spark://DXIDC7GBBIGDATAN39:7077 --class data.spark.batch.bankreport.app.ReportBankApp --executor-memory 10g --driver-memory 10g --total-executor-cores 20 /home/bigdata/ranzechen/reportBank.jar &> /data/REPORT_SPARK_LOG/"+''+rtt_table+".txt")
                                    if bankStatus == 0:
                                        bankinfo="spark make report success！！！"
                                        os.system("/home/bigdata/ranzechen/test.sh > /data/send_report.log")
                                        os.system("sh /home/bigdata/ranzechen/ftp.sh")
                                        bank_str="0000#make report is: "+rtt_table+">>>"+"success..."
                                        enc="UTF-8"
                                        bank_encoded = ''.join(bank_str).encode(enc)
                                        f = io.BytesIO()
                                        f.write(bank_encoded)
                                        f.seek(0)
                                        self.send_response(200)
                                        self.send_header("Content-type", "text/html;charset=%s" % enc)
                                        self.send_header("Content-Length", str(len(bank_encoded)))
                                        self.end_headers()
                                        shutil.copyfileobj(f,self.wfile)
                                    else:
                                        bankinfo="spark make report fail！！！"
                                        os.system("echo"+''+" spark执行结果:"+''+str(bankinfo))
                                        bank_str="0000#make report is: "+rtt_table+">>>"+"fail..."
                                        enc="UTF-8"
                                        bank_encoded = ''.join(bank_str).encode(enc)
                                        f = io.BytesIO()
                                        f.write(bank_encoded)
                                        f.seek(0)
                                        self.send_response(200)
                                        self.send_header("Content-type", "text/html;charset=%s" % enc)
                                        self.send_header("Content-Length", str(len(bank_encoded)))
                                        self.end_headers()
                                        shutil.copyfileobj(f,self.wfile)
                                else:
                                    os.system("echo "+''+str(datafrom)+''+">>/home/bigdata/ranzechen/conf.properties")
                                    os.system("sed -i /tfmccode/d /home/bigdata/ranzechen/conf.properties")
                                    tfmccode=os.popen("echo "+report_table_tfmccode +"| awk -F '/' '{print $3}'").read().strip('\n')
                                    os.system("echo "+''+str(tfmccode)+''+">>/home/bigdata/ranzechen/conf.properties")
                                    rtt_spark=os.system("/usr/local/spark/bin/spark-submit --master spark://DXIDC7GBBIGDATAN39:7077 --class com.spark.report.main.FirstSparkApp --executor-memory 10g --driver-memory  10g --total-executor-cores 20 /home/bigdata/ranzechen/report.jar &> /data/REPORT_SPARK_LOG/"+''+rtt_table+".txt")
                                    if rtt_spark == 0:
                                        rtt_spark_info="spark make report success！！！"
                                        os.system("/home/bigdata/ranzechen/test.sh > /data/send_report.log")
                                        os.system("sh /home/bigdata/ranzechen/ftp.sh")
                                        rtt_status_str="0000#make report is: "+rtt_table+">>>"+"OK..."
                                        enc="UTF-8"
                                        rtt_encoded = ''.join(rtt_status_str).encode(enc)
                                        f = io.BytesIO()
                                        f.write(rtt_encoded)
                                        f.seek(0)
                                        self.send_response(200)
                                        self.send_header("Content-type", "text/html;charset=%s" % enc)
                                        self.send_header("Content-Length", str(len(rtt_encoded)))
                                        self.end_headers()
                                        shutil.copyfileobj(f,self.wfile)
                                    else:
                                        rtt_spark_info="spark make report fail！！！"
                                        rtt_status_str="0000#make report is: "+rtt_table+">>>"+"fail..."
                                        enc="UTF-8"
                                        rtt_encoded = ''.join(rtt_status_str).encode(enc)
                                        f = io.BytesIO()
                                        f.write(rtt_encoded)
                                        f.seek(0)
                                        self.send_response(200)
                                        self.send_header("Content-type", "text/html;charset=%s" % enc)
                                        self.send_header("Content-Length", str(len(rtt_encoded)))
                                        self.end_headers()
                                        shutil.copyfileobj(f,self.wfile)
                        else:
                            rtt_no_str="0002#The table not exist !"
                            enc="UTF-8"
                            rtt_noencoded = ''.join(rtt_no_str).encode(enc)
                            f = io.BytesIO()
                            f.write(rtt_noencoded)
                            f.seek(0)
                            self.send_response(200)
                            self.send_header("Content-type", "text/html;charset=%s" % enc)
                            self.send_header("Content-Length", str(len(rtt_noencoded)))
                            self.end_headers()
                            shutil.copyfileobj(f,self.wfile)
                            os.system("echo"+''+" ..........................................................................") 
                    else:
                        mccodeNumStr="0002#Please input correct tfmccode!!!"
                        enc="UTF-8"
                        mccodeNumed = ''.join(mccodeNumStr).encode(enc)
                        f = io.BytesIO()
                        f.write(mccodeNumed)
                        f.seek(0)
                        self.send_response(200)
                        self.send_header("Content-type", "text/html;charset=%s" % enc)
                        self.send_header("Content-Length", str(len(mccodeNumed)))
                        self.end_headers()
                        shutil.copyfileobj(f,self.wfile)
                        os.system("echo"+''+" ..........................................................................")
                else:
                    df_no_str="0002#Please input correct datafrom!!!"
                    enc="UTF-8"
                    df_noencoded = ''.join(df_no_str).encode(enc)
                    f = io.BytesIO()
                    f.write(df_noencoded)
                    f.seek(0)
                    self.send_response(200)
                    self.send_header("Content-type", "text/html;charset=%s" % enc)
                    self.send_header("Content-Length", str(len(df_noencoded)))
                    self.end_headers()
                    shutil.copyfileobj(f,self.wfile)
                    os.system("echo"+''+" ..........................................................................")
#############oracle(ygbx)--->hdfs(ygbx)--->es(ygbx)
            elif "ygbx_es" in params:
                ygbx_es=str(bytes(params["ygbx_es"][0],'iso-8859-1'),'GBK')
                ygbx_es = params["ygbx_es"][0] if "ygbx_es" in params else None
                targetdir=os.popen("echo " + ygbx_es + "| awk -F '_' '{print $3}' | cut -b 1-4").read().strip('\n')
                os.system("echo"+''+" ..........................................................................")
                ygbx_path=os.popen("hadoop fs -ls /data/ybs_sett/"+''+targetdir+"/"+''+ygbx_es+"|awk '{print $8}'").read().strip('\n') 
                os.system("echo"+''+" ygbx_path:"+ygbx_path)
                os.system("/usr/local/spark/bin/spark-submit --master spark://DXIDC7GBBIGDATAN39:7077 --name ScalaEs --class com.spark.scala.es.ScalaEs  --executor-memory 30g --total-executor-cores 20 --driver-memory 30g /home/bigdata/ranzechen/ygbx/ygbx_es.jar hdfs://DXIDC7GBBIGDATAN39:8020"+''+ygbx_path+"&> /data/YGBX_SPARK_LOG/"+''+ygbx_es+".txt")
                ygbx_str2="0000#ygbx data is complete!!!"
                enc="UTF-8"
                ygbx_encoded2 = ''.join(ygbx_str2).encode(enc)
                f = io.BytesIO()
                f.write(ygbx_encoded2)
                f.seek(0)
                self.send_response(200)
                self.send_header("Content-type", "text/html;charset=%s" % enc)
                self.send_header("Content-Length", str(len(ygbx_encoded2)))
                self.end_headers()
                shutil.copyfileobj(f,self.wfile)       
#############oracle--->hdfs--->es,插入es模块
            elif "tablename" in params:
                sqooppidnum=os.popen("ps -ef | grep "+"\"sqoop\""+ "| wc -l").read().strip('\n')
                sparkpidnum=os.popen("ps -ef | grep data.spark.batch-0.0.1-SNAPSHOT.jar | wc -l").read().strip('\n')
                if int(sqooppidnum) <= 2 and int(sparkpidnum) <= 2:
                    tablename=str(bytes(params["tablename"][0],'iso-8859-1'),'GBK') 
                    tablename = params["tablename"][0] if "tablename" in params else None
                    #执行list-tables判断是否存在这张表 
                    sqoop=os.popen("sqoop list-tables --connect jdbc:oracle:thin:@100.1.1.203:1521:ORCL --username JFZXMG --password  WiKtZIoeqp4ktOALEEoZQpEnLSx7Il | grep -w "+tablename,'w')
                    sqoop_rc=sqoop.close()
                    print(sqoop_rc)
                    if sqoop_rc == None or sqoop_rc % 256:
                        status_str="0000#tablename is: "+tablename+">>>"+"OK..."
                        enc="UTF-8"
                        encoded = ''.join(status_str).encode(enc)
                        f = io.BytesIO()
                        f.write(encoded)
                        f.seek(0)
                        self.send_response(200)
                        self.send_header("Content-type", "text/html;charset=%s" % enc)
                        self.send_header("Content-Length", str(len(encoded)))
                        self.end_headers()
                        shutil.copyfileobj(f,self.wfile)
                        #获取sqoop导入的目录
                        targetdir=os.popen("echo " + tablename + "| awk -F '_' '{print $3}' | cut -b 1-4").read().strip('\n')
                        os.system("echo"+''+" ..........................................................................")
                        os.system("echo"+''+" 表名:"+''+tablename)
                        os.system("echo"+''+" 目录:/data/ybs_sett/"+''+targetdir)
                        starttime = datetime.datetime.now()
                        hdfspath=os.popen("hadoop fs -ls /data/ybs_sett/" + targetdir+"| grep -w "+tablename+"|awk '{print $8}'").read().strip('\n')
                        if hdfspath != "":
                            sparkstarttime=datetime.datetime.now()
                            #执行spark命令
                            spark_status=os.popen("/usr/local/spark/bin/spark-submit --master spark://DXIDC7GBBIGDATAN39:7077  --class data.spark.batch.oracledbes.app.YbsDataToES --executor-memory 10g --total-executor-cores 20  --driver-memory 10g  /home/bigdata/ranzechen/data.spark.batch-0.0.1-SNAPSHOT.jar hdfs://DXIDC7GBBIGDATAN39:8020"+''+hdfspath+" &> /data/SPARK_LOG/"+''+tablename+".txt",'w')
                            spark_status_rc=spark_status.close()
                            sparkendtime=datetime.datetime.now()
                            sparktime=(sparkendtime - sparkstarttime).seconds
                            if spark_status_rc == None or spark_status_rc % 256:
                                os.system("echo"+''+" spark插入es执行成功！！！")
                            else:
                                os.system("echo"+''+" spark插入es执行失败！！！")
                                os.system("echo"+''+" start make splitFileIntoEs.sh")
                                os.system("sh /home/bigdata/ranzechen/splitFileIntoEs.sh "+''+tablename+"> /data/SPARK_LOG/"+''+tablename+".txt")
                                os.system("echo"+''+" finish make splitFileIntoEs.sh")
                            os.system("echo"+''+" spark执行了"+''+str(sparktime)+"s")
                            os.system("echo"+''+" ..........................................................................")
                        else:
                            #执行sqoop命令
                            sqoop_status=os.popen("sqoop import --append --connect jdbc:oracle:thin:@100.1.1.203:1521:ORCL --username JFZXMG --password WiKtZIoeqp4ktOALEEoZQpEnLSx7Il  --fields-terminated-by \"衚\"  --hive-drop-import-delims --lines-terminated-by \"\n\" --target-dir /data/ybs_sett/"+''+targetdir+" --table "+''+tablename+" --verbose -m 1 &> /data/SQOOP_LOG/"+''+tablename+".txt",'w')
                            sqoop_status_rc=sqoop_status.close()
                            #判断sqoop是否成功执行
                            if sqoop_status_rc == None or sqoop_status_rc % 256:
                                sqoopinfo="sqoop 执行成功！！！"
                            else:
                                sqoopinfo="sqoop 执行失败！！！"
                                os.popen("sqoop import --append --connect jdbc:oracle:thin:@100.1.1.203:1521:ORCL --username JFZXMG --password WiKtZIoeqp4ktOALEEoZQpEnLSx7Il  --fields-terminated-by \"衚\"  --hive-drop-import-delims --lines-terminated-by \"\n\" --target-dir /data/ybs_sett/"+''+targetdir+" --table "+''+tablename+" --verbose -m 1 &> /data/SQOOP_LOG/"+''+tablename+".txt",'w')
                            os.system("echo"+''+" sqoop执行结果:"+''+str(sqoopinfo))
                            endtime = datetime.datetime.now()
                            #获取sqoop执行的时间差,秒
                            time=(endtime - starttime).seconds
                            os.system("echo"+''+" sqoop执行时间:"+''+str(time)+"s")
                            part=os.popen("grep repartitioned /data/SQOOP_LOG/"+''+tablename+".txt | awk '{print $9}'").read().strip('\n')
                            os.system("hadoop fs -mv /data/ybs_sett/"+''+targetdir+"/"+''+part+" /data/ybs_sett/"+''+targetdir+"/"+''+tablename)
                            path=os.popen("hadoop fs -ls /data/ybs_sett/" + targetdir + "/"+tablename+"|awk '{print $8}'").read().strip('\n')
                            os.system("echo"+''+" 路径为:"+''+str(path)) 
                            sparkstarttime=datetime.datetime.now()
                            #执行spark命令
                            spark_status=os.popen("/usr/local/spark/bin/spark-submit --master spark://DXIDC7GBBIGDATAN39:7077  --class data.spark.batch.oracledbes.app.YbsDataToES --executor-memory 10g --total-executor-cores 20  --driver-memory 10g  /home/bigdata/ranzechen/data.spark.batch-0.0.1-SNAPSHOT.jar hdfs://DXIDC7GBBIGDATAN39:8020"+''+path+" &> /data/SPARK_LOG/"+''+tablename+".txt",'w')
                            spark_status_rc=spark_status.close()
                            sparkendtime=datetime.datetime.now()
                            sparktime=(sparkendtime - sparkstarttime).seconds
                            if spark_status_rc == None or spark_status_rc % 256:
                                os.system("echo"+''+" spark插入es执行成功！！！")
                            else:
                                os.system("echo"+''+" spark插入es执行失败！！！")
                                os.system("echo"+''+" start make splitFileIntoEs.sh")
                                os.system("sh /home/bigdata/ranzechen/splitFileIntoEs.sh "+''+tablename+"> /data/SPARK_LOG/"+''+tablename+".txt")
                                os.system("echo"+''+" finish make splitFileIntoEs.sh")  
                            os.system("echo"+''+" spark执行了"+''+str(sparktime)+"s")
                            os.system("echo"+''+" ..........................................................................")
                            #如果不存在则页面返回不存在并退出
                    else:
                        no_str="0002#The table not exist !"
                        enc="UTF-8"
                        noencoded = ''.join(no_str).encode(enc)
                        f = io.BytesIO()
                        f.write(noencoded)
                        f.seek(0)
                        self.send_response(200)
                        self.send_header("Content-type", "text/html;charset=%s" % enc)
                        self.send_header("Content-Length", str(len(noencoded)))
                        self.end_headers()
                        shutil.copyfileobj(f,self.wfile)
                        os.system("echo"+''+" ..........................................................................")
                else:
                    pid_str="0001#busy.................."
                    enc="UTF-8"
                    pid_encoded = ''.join(pid_str).encode(enc)
                    f = io.BytesIO()
                    f.write(pid_encoded)
                    f.seek(0)
                    self.send_response(200)
                    self.send_header("Content-type", "text/html;charset=%s" % enc)
                    self.send_header("Content-Length", str(len(pid_encoded)))
                    self.end_headers()
                    shutil.copyfileobj(f,self.wfile)
#############通过curl来生成通用报表商户名称,生成文件的名称,清算数据名称
            elif "GeneralReportDataName" in params:
                GeneralReportDataName=str(bytes(params["GeneralReportDataName"][0],'iso-8859-1'),'GBK')
				generalDataName = params["GeneralReportDataName"][0] if "GeneralReportDataName" in params else None
                if len(generalDataName.split("/")) == 3:
                    ybsDataName=generalDataName.split("/")[0]
                    mccodeName=generalDataName.split("/")[1].split("=")[1]
                    excelName=generalDataName.split("/")[2].split("=")[1]
                    year=ybsDataName.split("_")[2][:4]
                    hdfsPathStatus=os.popen("hadoop fs -du /data/ybs_sett/"+year+"/"+ybsDataName).read().strip('\n')
                    if hdfsPathStatus != '':
                        hdfsPath=os.popen("hadoop fs -du /data/ybs_sett/"+year+"/"+ybsDataName).read().strip('\n').split()[1]
                        os.system("sed -i /inputPath/d /home/bigdata/ranzechen/gllsConf.properties")
                        os.system("sed -i /excelName/d /home/bigdata/ranzechen/gllsConf.properties")
                        os.system("sed -i /mccodeName/d /home/bigdata/ranzechen/gllsConf.properties")
                        os.system("echo inputPath=hdfs://DXIDC7GBBIGDATAN39:8020"+''+str(hdfsPath)+''+ ">>/home/bigdata/ranzechen/gllsConf.properties ")
                        os.system("echo mccodeName="+''+str(mccodeName)+''+">>/home/bigdata/ranzechen/gllsConf.properties")
                        os.system("echo excelName="+''+str(excelName)+''+">>/home/bigdata/ranzechen/gllsConf.properties")
                        generalReport=os.popen("/usr/local/spark/bin/spark-submit --master spark://DXIDC7GBBIGDATAN39:7077  --class data.spark.batch.glls_report.app.GLLSReportApp --executor-memory 10g --total-executor-cores 20  --driver-memory 10g  /home/bigdata/ranzechen/GeneralReport.jar &> /data/GeneralReportLog/"+''+mccodeName+".txt")
						
                        generalSparkResStr="0000#make report is success"
                        enc="UTF-8"
                        generalSparkEncoded= ''.join(generalSparkResStr).encode(enc)
                        f = io.BytesIO()
                        f.write(generalSparkEncoded)
                        f.seek(0)
                        self.send_response(200)
                        self.send_header("Content-type", "text/html;charset=%s" % enc)
                        self.send_header("Content-Length", str(len(generalSparkEncoded)))
                        self.end_headers()
                        shutil.copyfileobj(f,self.wfile)
                    else:
                        noExistStr="0002#The "+ybsDataName+" not exist !"
                        enc="UTF-8"
                        noExistencoded= ''.join(noExistStr).encode(enc)
                        f = io.BytesIO()
                        f.write(noExistencoded)
                        f.seek(0)
                        self.send_response(200)
                        self.send_header("Content-type", "text/html;charset=%s" % enc)
                        self.send_header("Content-Length", str(len(noExistencoded)))
                        self.end_headers()
                        shutil.copyfileobj(f,self.wfile)
                else:
                    errorStr="0002#Please input correct parameters!!!"
                    enc="UTF-8"
                    errorEncoded=''.join(errorStr).encode(enc)
                    f = io.BytesIO()
                    f.write(errorEncoded)
                    f.seek(0)
                    self.send_response(200)
                    self.send_header("Content-type", "text/html;charset=%s" % enc)
                    self.send_header("Content-Length", str(len(errorEncoded)))
                    self.end_headers()
                    shutil.copyfileobj(f,self.wfile)
            #添加功能代码       
#######################################################
            else:
                element_str="0002#Please input correct parameters!!!"
                enc="UTF-8"
                element_encoded = ''.join(element_str).encode(enc)
                f = io.BytesIO()
                f.write(element_encoded)
                f.seek(0)
                self.send_response(200)
                self.send_header("Content-type", "text/html;charset=%s" % enc)
                self.send_header("Content-Length", str(len(element_encoded)))
                self.end_headers()
                shutil.copyfileobj(f,self.wfile)
        else:
            fomatErrorStr="0002#Please input correct parameters!!!"
            enc="UTF-8"
            formatEncoded = ''.join(fomatErrorStr).encode(enc)
            f = io.BytesIO()
            f.write(formatEncoded)
            f.seek(0)
            self.send_response(200)
            self.send_header("Content-type", "text/html;charset=%s" % enc)
            self.send_header("Content-Length", str(len(formatEncoded)))
            self.end_headers()
            shutil.copyfileobj(f,self.wfile)            
    # 解析?并将读入的行转为UTF-8
    def do_POST(self):
        s = str(self.rfile.readline(), 'UTF-8')
        print(urllib.parse.parse_qs(urllib.parse.unquote(s)))
        self.send_response(301)
        self.send_header("Location", "/?" + s)
        self.end_headers()
    
class ThreadingHttpServer(ThreadingMixIn, HTTPServer):
    # pass是空语句，是为了保持程序结构的完整性,不做任何事情，一般用做占位语句
    pass
# 设置url地址和端口号
httpd = ThreadingHttpServer(('100.1.1.32', 9999), MyHttpHandler)
print("server start on .....")
httpd.serve_forever()

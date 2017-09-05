# !coding=UTF-8
'''
@author: ranzechen
@version: python2

@function: 
1.Query the data in ybsSett/ygbx es to calculate the total amount by index and type
2.Sqoop export data from oracle to HDFS
3.Generate all the merchant reports based on the YBS_SETT_yyyymmdd
4.Generate some the merchant reports based on the YBS_SETT_yyyymmdd and merchant and datafrom
5.Insert ygbx data to es
6.Insert ybsSett data to es
7.Generate general report

'''
from BaseHTTPServer import HTTPServer,BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
import urlparse,io,os,json,shutil
from  datetime  import  *
#diff:python3 import urllib;urlparse <===> import uslparse; usrllib.parse

class ServerForYBS(BaseHTTPRequestHandler):

    YbsSett_ES_IP_PORT = '10.117.1.32:9200'
    Ygbx_ES_IP_PORT = '10.117.1.32:9200'
    SQOOP_IP_PORT = '10.117.0.217:1521'
    SQOOP_USER = 'jfzxmg'
    SQOOP_PASSWD = 'test'
    YbsReportConf_Path = '/home/storm/ranzechen/ReportConf.properties'
    GeneralReportConf = '/home/storm/ranzechen/gllsConf.properties'
    HdfsMaster_IP_PORT = '10.117.0.217:9000'
    SparkMaster_IP_PORT = '10.117.1.32:7077'
    Spark_JAR_PATH = '/home/storm/ranzechen/data.spark.batch-0.0.1-SNAPSHOT.jar' 
    #must add / to next dir
    SQOOP_LOG = '/home/storm/ranzechen/SQOOP_LOG/'
    SparkGeneralReportLog = '/home/storm/ranzechen/SPARK_LOG/GeneralReportLog/'
    SparkYbsReportLog = '/home/storm/ranzechen/SPARK_LOG/YbsReportLog/'
    SparkInsertEsLog = '/home/storm/ranzechen/SPARK_LOG/InsertEsLog/'

    FlagLine = '*************************************************************************************************'
    def do_GET(self):
        if '?' in self.path:
            self.queryString = urlparse.unquote(self.path.split('?', 1)[1])
            params = urlparse.parse_qs(self.queryString)
# Query the data in ybsSett es to calculate the total amount by index and type
            if 'queryYbsSett' in params:
                ybsEsType = params['queryYbsSett'][0]
                ybsEsIndex = 'test_ybs_sett_'+ybsEsType[:6]
                if len(ybsEsType) == 8:
                    self.LogOutput(params)
                    self.QueryEs(ybsEsIndex,ybsEsType,self.YbsSett_ES_IP_PORT)
                else:
                    self.ErrorParameters()
# Query the data in ygbx es to calculate the total amount by index and type
            elif 'queryYgbx' in params:
                  ygbxEsType = params['queryYgbx'][0]
                  ygbxEsIndex = 'ygbx_sett_'+ygbxEsType[:6]
                  if len(ygbxEsType) == 8:
                      self.LogOutput(params)
                      self.QueryEs(ygbxEsIndex,ygbxEsType,self.Ygbx_ES_IP_PORT)
                  else:
                      self.ErrorParameters()
# Sqoop data from oracle to hdfs
            elif 'sqoopTable' in params:
                  tableName = params['sqoopTable'][0]
                  if 'YBS_SETT_' in tableName and len(tableName.split('_')[2]) ==8:
                      sqoopProcessNum = os.popen('ps -aux | grep sqoop | wc -l').read().strip('\n')
                      if int(sqoopProcessNum) < 4:
                          if self.SqoopIsExistsTable(tableName):
                              sqoopStr="0000#Sqoop table success!!!"
                              enc="UTF-8"
                              sqoopEncoded = ''.join(sqoopStr).encode(enc)
                              f = io.BytesIO()
                              f.write(sqoopEncoded)
                              f.seek(0)
                              self.send_response(200)
                              self.send_header("Content-type", "text/html;charset=%s" % enc)
                              self.send_header("Content-Length", str(len(sqoopEncoded)))
                              self.end_headers()
                              shutil.copyfileobj(f,self.wfile)

                              self.LogOutput(params)
                              self.SqoopData(tableName)
                          else:
                              self.SqoopNotExistTable()
                      else:
                          self.ProcessFun()
                  else:
                      self.ErrorParameters()
# Generate all the merchant reports based on the YBS_SETT_yyyymmdd
            elif 'generateAllReport' in params:
                  tableName = params['generateAllReport'][0]
                  if 'YBS_SETT_' in tableName and len(tableName.split('_')[2]) ==8:
                      sqoopProcessNum = os.popen('ps -aux | grep sqoop | wc -l').read().strip('\n')
                      if int(sqoopProcessNum) < 3:
                          sparkProcessNum = os.popen('ps -aux | grep data.spark.batch.report.app.ReportApp | wc -l').read().strip('\n')
                          if int(sparkProcessNum) < 3:#ServerForYbs 1 + ReportApp 1 + againServerForYbs 1 + grep 1 = 4
                              self.LogOutput(params)
                              self.GenerateAllReport(tableName) 
                          else:
                              self.ProcessFun()
                      else:
                          self.ProcessFun()
                  else:
                      self.ErrorParameters()
# Generate some the reports
            elif 'generatePartReport' in params and 'tfmccode' in params and 'datafrom' in params:            
                  tableName = params['generatePartReport'][0]
                  tfmccode = params['tfmccode'][0]
                  datafrom = params['datafrom'][0]
                  if 'YBS_SETT_' in tableName and len(tableName.split('_')[2]) == 8 and ('jiaofei' == datafrom or 'bank' == datafrom or 'zhilian' == datafrom or 'all' == datafrom):
                      sqoopProcessNum = os.popen('ps -aux | grep sqoop | wc -l').read().strip('\n')
                      if int(sqoopProcessNum) < 3:
                          sparkProcessNum = os.popen('ps -aux | grep data.spark.batch.report.app.ReportApp | wc -l').read().strip('\n')
                          if int(sparkProcessNum) < 3:
                              self.LogOutput(params)
                              self.GeneratePartReport(tableName,tfmccode,datafrom)
                          else:
                              self.ProcessFun()               
                      else:
                          self.ProcessFun()
                  else:
                      self.ErrorParameters()
# Insert ygbx data to es
            elif 'insertYgbx' in params:
                  tableName = params['insertYgbx'][0]
                  if 'YBS_SETT_' in tableName and len(tableName.split('_')[2]) == 8:
                      sqoopProcessNum = os.popen('ps -aux | grep sqoop | wc -l').read().strip('\n')
                      if int(sqoopProcessNum) < 3:
                          sparkProcessNum = os.popen('ps -aux | grep com.spark.scala.es.ScalaEs | wc -l').read().strip('\n')
                          if int(sparkProcessNum) <3:
                              self.LogOutput(params)
                              self.InsertYgbxData(tableName)
                          else:
                              self.ProcessFun()
                      else:
                          self.ProcessFun()    
                  else:
                      self.ErrorParameters()
# Insert ybsSett data to es
            elif 'insertYbsSett' in params:
                  tableName = params['insertYbsSett'][0]
                  if 'YBS_SETT_' in tableName and len(tableName.split('_')[2]) == 8:
                      sqoopProcessNum = os.popen('ps -aux | grep sqoop | wc -l').read().strip('\n')
                      if int(sqoopProcessNum) < 3:
                          sparkProcessNum = os.popen('ps -aux | grep com.spark.scala.es.ScalaEs | wc -l').read().strip('\n')
                          if int(sparkProcessNum) <3:
                              self.LogOutput(params)
                              self.InsertYbsSettData(tableName)
                          else:
                              self.ProcessFun()
                      else:
                          self.ProcessFun()
                  else:
                      self.ErrorParameters()
# Generate general report
            elif 'generateGeneralReport' in params and 'tfmccode' in params and 'excelName' in params:
                  tableName = params['generateGeneralReport'][0]
                  tfmccode = params['tfmccode'][0]
                  excelName = params['excelName'][0]
                  if 'YBS_SETT_' in tableName and len(tableName.split('_')[2]) == 8:
                      sqoopProcessNum = os.popen('ps -aux | grep sqoop | wc -l').read().strip('\n')
                      if int(sqoopProcessNum) < 3:
                          sparkProcessNum = os.popen('ps -aux | data.spark.batch.glls_report.app.GLLSReportApp | wc -l').read().strip('\n')
                          if int(sparkProcessNum) < 3:
                              self.LogOutput(params)
                              self.GenerateGeneralReport(tableName,tfmccode,excelName)
                          else:
                              self.ProcessFun()
                      else:
                          self.ProcessFun()
                  else:
                      self.ErrorParameters()
# Add new Function 
            else:
                self.ErrorParameters()               
        else:
            self.ErrorParameters()
 
# Function_1: Query ES Data
    def QueryEs(self,esIndex,esType,ES_IP_PORT):
        esQueryRes=os.popen("curl -XGET http://"+ ES_IP_PORT +"/_search -d'{" + "\"from\"" + ":0," + "\"size\"" + ":0," + "\"query\"" + ":{" + "\"bool\"" + ":{" + "\"must\"" + ":[{" + "\"match\"" + ":{" + "\"_index\"" + ":" + "\"" + esIndex + "\"" + "}},{" + "\"match\"" + ":{" + "\"_type\"" + ":" + "\"" + esType + "\"" + "}},{" + "\"match\"" + ":{" + "\"sett_flag\"" + ":" + "\"1\"" + "}}]}}," + "\"aggs\"" + ":{" + "\"intraday_return\"" + ":{" + "\"sum\"" + ":{" + "\"field\"" + ":" + "\"tftxmony\"" + "}}}}'").read().strip('\n')        
        esResJson = json.loads(esQueryRes)
        cnt = esResJson['hits']['total']
        money = esResJson['aggregations']['intraday_return']['value']
        esQueryStr = "0000#DATE=" + esType + ",CNT=" + str(cnt) + ",MONEY=" + str('%.2f' % (money))
        enc = "UTF-8"
        esQueryEncoded = ''.join(esQueryStr).encode(enc)
        f = io.BytesIO()
        f.write(esQueryEncoded)
        f.seek(0)
        self.send_response(200)
        self.send_header("Content-type", "text/html;charset=%s" % enc)
        self.send_header("Content-Length", str(len(esQueryEncoded)))
        self.end_headers()
        shutil.copyfileobj(f, self.wfile)

# Function_2: Sqoop data from oracle to hdfs
    def SqoopData(self,tableName):
        year = tableName.split('_')[2][:4]
        sqoopDataStartTime = datetime.now()
        sqoopDataStatus = os.system("sqoop import --append --connect jdbc:oracle:thin:@"+ self.SQOOP_IP_PORT +":ORCL --username "+ self.SQOOP_USER +" --password "+ self.SQOOP_PASSWD +"  --fields-terminated-by \"衚\"  --hive-drop-import-delims --lines-terminated-by \"\n\" --target-dir /data/ybs_sett/"+ year +" --verbose -m 1 --table "+ tableName +" > "+ self.SQOOP_LOG + tableName +'.log 2>&1')
        if sqoopDataStatus == 0:
            os.system('hadoop fs -mv /data/ybs_sett/'+year+'/part-m-00000 /data/ybs_sett/'+year+'/'+tableName)
            sqoopDataEndTime = datetime.now()
            print('>>>>>Sqoop Used Time: %.2f' % ((sqoopDataEndTime - sqoopDataStartTime).total_seconds())+' s')
            print('>>>>>Sqoop Table Success!!!')
        else:
            print('>>>>>Sqoop Table Failed!!!')

# Function_3: Generate all report
    def GenerateAllReport(self,tableName):
        year = tableName.split('_')[2][:4]
        hdfsStatus = os.popen('hadoop fs -du /data/ybs_sett/'+year+'/'+tableName).read().strip('\n')
        if hdfsStatus != '':
            hdfsPath = hdfsStatus.split()[1]
            os.system('sed -i /settFilePath/d '+ self.YbsReportConf_Path)
            os.system('sed -i /tfmccode/d '+ self.YbsReportConf_Path)
            os.system('echo tfmccode=all >> '+ self.YbsReportConf_Path)
            os.system('sed -i /datafrom/d '+ self.YbsReportConf_Path)
            os.system('echo datafrom=all >> '+ self.YbsReportConf_Path)
            os.system('echo settFilePath=hdfs://'+ self.HdfsMaster_IP_PORT + hdfsPath +' >> ' + self.YbsReportConf_Path)
            
            allSparkStr = '0000#Generate all report success!'
            enc="UTF-8"
            allSparkEncoded = ''.join(allSparkStr).encode(enc)
            f = io.BytesIO()
            f.write(allSparkEncoded)
            f.seek(0)
            self.send_response(200)
            self.send_header("Content-type", "text/html;charset=%s" % enc)
            self.send_header("Content-Length", str(len(allSparkEncoded)))
            self.end_headers()
            shutil.copyfileobj(f,self.wfile)

            sparkStatus = os.system('/usr/local/spark/bin/spark-submit --master spark://'+ self.SparkMaster_IP_PORT +' --class data.spark.batch.report.app.ReportApp '+self.Spark_JAR_PATH +' &> '+ self.SparkYbsReportLog + tableName+'.log')
            if sparkStatus == 0:
                print('>>>>>Generate All Report Success!!!')
            else:
                print('>>>>>Generate All Report Failed!!!')
 
        else:
            if self.SqoopIsExistsTable(tableName):
                sqoopReturnStatus = '0000#Sqoop table success!!!'
                allSparkStr = sqoopReturnStatus+'&0000#Generate all report success!!!'
                enc = "UTF-8"
                allSparkEncoded = ''.join(allSparkStr).encode(enc)
                f = io.BytesIO()
                f.write(allSparkEncoded)
                f.seek(0)
                self.send_response(200)
                self.send_header("Content-type", "text/html;charset=%s" % enc)
                self.send_header("Content-Length", str(len(allSparkEncoded)))
                self.end_headers()
                shutil.copyfileobj(f, self.wfile) 
          
                self.SqoopData(tableName)#sqoop data

                hdfsPath = os.popen('hadoop fs -du /data/ybs_sett/'+year+'/'+tableName).read().strip('\n').split()[1]
                os.system('sed -i /settFilePath/d '+ self.YbsReportConf_Path)
                os.system('sed -i /tfmccode/d '+ self.YbsReportConf_Path)
                os.system('echo tfmccode=all >> '+ self.YbsReportConf_Path)
                os.system('sed -i /datafrom/d '+ self.YbsReportConf_Path)
                os.system('echo datafrom=all >> '+ self.YbsReportConf_Path)
                os.system('echo settFilePath=hdfs://'+ self.HdfsMaster_IP_PORT + hdfsPath +' >> ' + self.YbsReportConf_Path)
            
                sparkStatus = os.system('/usr/local/spark/bin/spark-submit --master spark://'+ self.SparkMaster_IP_PORT +' --class data.spark.batch.report.app.ReportApp '+self.Spark_JAR_PATH +' &> '+ self.SparkYbsReportLog + tableName+'.log')
                if sparkStatus == 0:
                    print('>>>>>Generate All Report Success!!!')
                else:
                    print('>>>>>Generate All Report Failed!!!')
            else:
                self.SqoopNotExistTable()

# Funciton_4: Generate some the reports
    def GeneratePartReport(self,tableName,tfmccode,datafrom):
        year = tableName.split('_')[2][:4]
        hdfsStatus = os.popen('hadoop fs -du /data/ybs_sett/'+year+'/'+tableName).read().strip('\n')
        if hdfsStatus != '':
            hdfsPath = hdfsStatus.split()[1]
            os.system('sed -i /settFilePath/d '+ self.YbsReportConf_Path)
            os.system('sed -i /tfmccode/d '+ self.YbsReportConf_Path)
            os.system('echo tfmccode='+tfmccode+' >> '+ self.YbsReportConf_Path)
            os.system('sed -i /datafrom/d '+ self.YbsReportConf_Path)
            os.system('echo datafrom='+datafrom+' >> '+ self.YbsReportConf_Path)
            os.system('echo settFilePath=hdfs://'+ self.HdfsMaster_IP_PORT + hdfsPath +' >> ' + self.YbsReportConf_Path) 

            partSparkStr = '0000#Generate part report success!'
            enc="UTF-8"
            partSparkEncoded = ''.join(partSparkStr).encode(enc)
            f = io.BytesIO()
            f.write(partSparkEncoded)
            f.seek(0)
            self.send_response(200)
            self.send_header("Content-type", "text/html;charset=%s" % enc)
            self.send_header("Content-Length", str(len(partSparkEncoded)))
            self.end_headers()
            shutil.copyfileobj(f,self.wfile)

            sparkStatus = os.system('/usr/local/spark/bin/spark-submit --master spark://'+ self.SparkMaster_IP_PORT +' --class data.spark.batch.report.app.ReportApp '+self.Spark_JAR_PATH +' &> '+ self.SparkYbsReportLog + tableName+'.log')
            if sparkStatus == 0:
                print('>>>>>Generate Part Report Success!!!')
            else:
                print('>>>>>Generate Part Report Failed!!!')
        else:
            if self.SqoopIsExistsTable(tableName):
                sqoopReturnStatus = '0000#Sqoop table success!!!'
                partSparkStr = sqoopReturnStatus+'&0000#Generate part report success!!!'
                enc = "UTF-8"
                partSparkEncoded = ''.join(partSparkStr).encode(enc)
                f = io.BytesIO()
                f.write(partSparkEncoded)
                f.seek(0)
                self.send_response(200)
                self.send_header("Content-type", "text/html;charset=%s" % enc)
                self.send_header("Content-Length", str(len(partSparkEncoded)))
                self.end_headers()
                shutil.copyfileobj(f, self.wfile)

                self.SqoopData(tableName)#sqoop data

                hdfsPath = os.popen('hadoop fs -du /data/ybs_sett/'+year+'/'+tableName).read().strip('\n').split()[1]
                os.system('sed -i /settFilePath/d '+ self.YbsReportConf_Path)
                os.system('sed -i /tfmccode/d '+ self.YbsReportConf_Path)
                os.system('echo tfmccode='+tfmccode+' >> '+ self.YbsReportConf_Path)
                os.system('sed -i /datafrom/d '+ self.YbsReportConf_Path)
                os.system('echo datafrom='+datafrom+' >> '+ self.YbsReportConf_Path)
                os.system('echo settFilePath=hdfs://'+ self.HdfsMaster_IP_PORT + hdfsPath +' >> ' + self.YbsReportConf_Path)

                sparkStatus = os.system('/usr/local/spark/bin/spark-submit --master spark://'+ self.SparkMaster_IP_PORT +' --class data.spark.batch.report.app.ReportApp '+self.Spark_JAR_PATH +' &> '+ self.SparkYbsReportLog + tableName+'.log')
                if sparkStatus == 0:
                    print('>>>>>Generate part Report Success!!!')
                else:
                    print('>>>>>Generate part Report Failed!!!')
            else:
                self.SqoopNotExistTable()

# Function_5: Insert ygbx data to es
    def InsertYgbxData(self,tableName):
        year = tableName.split('_')[2][:4]
        hdfsStatus = os.popen('hadoop fs -du /data/ybs_sett/'+year+'/'+tableName).read().strip('\n')
        if hdfsStatus != '':
            hdfsPath = hdfsStatus.split()[1]

            insertYgbxSparkStr = '0000#Insert ygbx data success!!!'
            enc="UTF-8"
            insertYgbxSparkEncoded = ''.join(insertYgbxSparkStr).encode(enc)
            f = io.BytesIO()
            f.write(insertYgbxSparkEncoded)
            f.seek(0)
            self.send_response(200)
            self.send_header("Content-type", "text/html;charset=%s" % enc)
            self.send_header("Content-Length", str(len(insertYgbxSparkEncoded)))
            self.end_headers()
            shutil.copyfileobj(f,self.wfile)
           
            sparkStatus = os.system('/usr/local/spark/bin/spark-submit --master spark://'+ self.SparkMaster_IP_PORT +' --class com.spark.scala.es.ScalaEs '+self.Spark_JAR_PATH +' hdfs://'+ self.HdfsMaster_IP_PORT + hdfsPath  +' &> '+ self.SparkInsertEsLog + tableName+'.log')
            if sparkStatus == 0:
                print('>>>>>Insert Ygbx Data Success!!!')
            else:
                print('>>>>>Insert Ygbx Data Failed!!!')

        else:
            if self.SqoopIsExistsTable(tableName):
                sqoopReturnStatus = '0000#Sqoop table success!!!'
                insertYgbxSparkStr = sqoopReturnStatus+'&0000#Insert ygbx data success!!!'
                enc = "UTF-8"
                insertYgbxSparkEncoded = ''.join(insertYgbxSparkStr).encode(enc)
                f = io.BytesIO()
                f.write(insertYgbxSparkEncoded)
                f.seek(0)
                self.send_response(200)
                self.send_header("Content-type", "text/html;charset=%s" % enc)
                self.send_header("Content-Length", str(len(insertYgbxSparkEncoded)))
                self.end_headers()
                shutil.copyfileobj(f, self.wfile)
                
                self.SqoopData(tableName)#sqoop data                
 
                hdfsPath = os.popen('hadoop fs -du /data/ybs_sett/'+year+'/'+tableName).read().strip('\n').split()[1]
                sparkStatus = os.system('/usr/local/spark/bin/spark-submit --master spark://'+ self.SparkMaster_IP_PORT +' --class com.spark.scala.es.ScalaEs '+self.Spark_JAR_PATH +' hdfs://'+ self.HdfsMaster_IP_PORT + hdfsPath  +' &> '+ self.SparkInsertEsLog + tableName+'.log')
                if sparkStatus == 0:
                    print('>>>>>Insert Ygbx Data Success!!!')
                else:
                    print('>>>>>Insert Ygbx Data Failed!!!')            
            else:
                self.SqoopNotExistTable()

# Function_6: Insert ybsSett data to es
    def InsertYbsSettData(self,tableName): 
        year = tableName.split('_')[2][:4]
        hdfsStatus = os.popen('hadoop fs -du /data/ybs_sett/'+year+'/'+tableName).read().strip('\n')
        if hdfsStatus != '':
            hdfsPath = hdfsStatus.split()[1]

            insertYbsSettSparkStr = '0000#Insert ybsSett data success!!!'
            enc="UTF-8"
            insertYbsSettSparkEncoded = ''.join(insertYbsSettSparkStr).encode(enc)
            f = io.BytesIO()
            f.write(insertYbsSettSparkEncoded)
            f.seek(0)
            self.send_response(200)
            self.send_header("Content-type", "text/html;charset=%s" % enc)
            self.send_header("Content-Length", str(len(insertYbsSettSparkEncoded)))
            self.end_headers()
            shutil.copyfileobj(f,self.wfile)
                  
            sparkStatus = os.system('/usr/local/spark/bin/spark-submit --master spark://'+ self.SparkMaster_IP_PORT +' --class data.spark.batch.oracledbes.app.YbsDataToES '+self.Spark_JAR_PATH +' hdfs://'+ self.HdfsMaster_IP_PORT + hdfsPath  +' &> '+ self.SparkInsertEsLog + tableName+'.log') 
            if sparkStatus == 0:
                print('>>>>>Insert YbsSett Data Success!!!')
            else:
                print('>>>>>Insert YbsSett Data Failed!!!')

        else:
            if self.SqoopIsExistsTable(tableName):
                sqoopReturnStatus = '0000#Sqoop table success!!!'
                insertYgbxSparkStr = sqoopReturnStatus+'&0000#Insert ybsSett data success!!!'
                enc = "UTF-8"
                insertYgbxSparkEncoded = ''.join(insertYgbxSparkStr).encode(enc)
                f = io.BytesIO()
                f.write(insertYgbxSparkEncoded)
                f.seek(0)
                self.send_response(200)
                self.send_header("Content-type", "text/html;charset=%s" % enc)
                self.send_header("Content-Length", str(len(insertYgbxSparkEncoded)))
                self.end_headers()
                shutil.copyfileobj(f, self.wfile)

                self.SqoopData(tableName)#sqoop data

                hdfsPath = os.popen('hadoop fs -du /data/ybs_sett/'+year+'/'+tableName).read().strip('\n').split()[1]
                sparkStatus = os.system('/usr/local/spark/bin/spark-submit --master spark://'+ self.SparkMaster_IP_PORT +' --class data.spark.batch.oracledbes.app.YbsDataToES '+self.Spark_JAR_PATH +' hdfs://'+ self.HdfsMaster_IP_PORT + hdfsPath  +' &> '+ self.SparkInsertEsLog + tableName+'.log')
                if sparkStatus == 0:
                    print('>>>>>Insert YbsSett Data Success!!!')
                else:
                    print('>>>>>Insert YbsSett Data Failed!!!')       
        
            else:
                self.SqoopNotExistTable()
  
# Function_7: Generate general report
    def GenerateGeneralReport(self,tableName,tfmccode,excelName):   
        year = tableName.split('_')[2][:4]
        hdfsStatus = os.popen('hadoop fs -du /data/ybs_sett/'+year+'/'+tableName).read().strip('\n')
        if hdfsStatus != '':
            hdfsPath = hdfsStatus.split()[1]
            os.system('sed -i /inputPath/d '+ self.GeneralReportConf_Path)
            os.system('sed -i /tfmccode/d '+ self.GeneralReportConf_Path)
            os.system('echo tfmccode='+tfmccode+' >> '+ self.GeneralReportConf_Path)
            os.system('sed -i /excelName/d '+ self.GeneralReportConf_Path)
            os.system('echo excelName='+excelName+' >> '+ self.GeneralReportConf_Path)
            os.system('echo inputPath=hdfs://'+ self.HdfsMaster_IP_PORT + hdfsPath +' >> ' + self.GeneralReportConf_Path)

            generalReportSparkStr = '0000#Generate general report success!'
            enc="UTF-8"
            generalReportSparkEncoded = ''.join(generalReportSparkStr).encode(enc)
            f = io.BytesIO()
            f.write(generalReportSparkEncoded)
            f.seek(0)
            self.send_response(200)
            self.send_header("Content-type", "text/html;charset=%s" % enc)
            self.send_header("Content-Length", str(len(generalReportSparkEncoded)))
            self.end_headers()
            shutil.copyfileobj(f,self.wfile)

            sparkStatus = os.system('/usr/local/spark/bin/spark-submit --master spark://'+ self.SparkMaster_IP_PORT +' --class data.spark.batch.glls_report.app.GLLSReportApp '+self.Spark_JAR_PATH +' &> '+ self.SparkGeneralReportLog + tableName+'.log')
            if sparkStatus == 0:
                print('>>>>>Generate General Report Success!!!')
            else:
                print('>>>>>Generate General Report Failed!!!')
        else:
            if self.SqoopIsExistsTable(tableName):
                sqoopReturnStatus = '0000#Sqoop table success!!!'
                generalReportSparkStr = sqoopReturnStatus+'&0000#Generate General report success!!!'
                enc = "UTF-8"
                generalReportSparkEncoded = ''.join(generalReportSparkStr).encode(enc)
                f = io.BytesIO()
                f.write(generalReportSparkEncoded)
                f.seek(0)
                self.send_response(200)
                self.send_header("Content-type", "text/html;charset=%s" % enc)
                self.send_header("Content-Length", str(len(generalReportSparkEncoded)))
                self.end_headers()
                shutil.copyfileobj(f, self.wfile)

                self.SqoopData(tableName)#sqoop data 

                hdfsPath = os.popen('hadoop fs -du /data/ybs_sett/'+year+'/'+tableName).read().strip('\n')
                os.system('sed -i /inputPath/d '+ self.GeneralReportConf_Path)
                os.system('sed -i /tfmccode/d '+ self.GeneralReportConf_Path)
                os.system('echo tfmccode='+tfmccode+' >> '+ self.GeneralReportConf_Path)
                os.system('sed -i /excelName/d '+ self.GeneralReportConf_Path)
                os.system('echo excelName='+excelName+' >> '+ self.GeneralReportConf_Path)
                os.system('echo inputPath=hdfs://'+ self.HdfsMaster_IP_PORT + hdfsPath +' >> ' + self.GeneralReportConf_Path)
            
                sparkStatus = os.system('/usr/local/spark/bin/spark-submit --master spark://'+ self.SparkMaster_IP_PORT +' --class data.spark.batch.glls_report.app.GLLSReportApp '+self.Spark_JAR_PATH +' &> '+ self.SparkGeneralReportLog + tableName+'.log')
                if sparkStatus == 0:
                    print('>>>>>Generate General Report Success!!!')
                else:
                    print('>>>>>Generate General Report Failed!!!')
            else:
                self.SqoopNotExistTable()
# Function_8:
 
# Process wait
    def ProcessFun(self):     
        processStr = '0001#The program is running,please wait!!!'
        enc = "UTF-8"
        processEncoded = ''.join(processStr).encode(enc)
        f = io.BytesIO()
        f.write(processEncoded)
        f.seek(0)
        self.send_response(200)
        self.send_header("Content-type", "text/html;charset=%s" % enc)
        self.send_header("Content-Length", str(len(processEncoded)))
        self.end_headers()
        shutil.copyfileobj(f, self.wfile)

# Funciton_2: Determine whether the table exists in oracle
    def SqoopIsExistsTable(self,tableName):
        sqoopQueryStatus = os.system('sqoop list-tables --connect jdbc:oracle:thin:@'+ self.SQOOP_IP_PORT +':ORCL --username '+ self.SQOOP_USER +' --password '+ self.SQOOP_PASSWD +' | grep -w '+tableName)
        if sqoopQueryStatus == 0:
            return True
        else:
            return False

# Sqoop table not exist 
    def SqoopNotExistTable(self):
        sqoopStatusStr = '0002#The table doesn\'t exist!!!'
        enc = "UTF-8"
        sqoopStatusEncoded = ''.join(sqoopStatusStr).encode(enc)
        f = io.BytesIO()
        f.write(sqoopStatusEncoded)
        f.seek(0)
        self.send_response(200)
        self.send_header("Content-type", "text/html;charset=%s" % enc)
        self.send_header("Content-Length", str(len(sqoopStatusEncoded)))
        self.end_headers()
        shutil.copyfileobj(f, self.wfile)

# Log output formatt
    def LogOutput(self,params):
        print(self.FlagLine)
        print('>>>>>Request Time: '+str(datetime.now().strftime( '%Y-%m-%d %H:%M:%S')))
        print('>>>>>Input Keys: '+str(params.keys()))
        print('>>>>>Input Values: '+str(params.values()))

# Error returning the incoming parameter
    def ErrorParameters(self):
        print(self.FlagLine)
        print('>>>>>Input Parameters Error!!')
        errorParametersStr="0002#Please input correct parameters!!!"
        enc="UTF-8"
        errorParametersEncoded = ''.join(errorParametersStr).encode(enc)
        f = io.BytesIO()
        f.write(errorParametersEncoded)
        f.seek(0)
        self.send_response(200)
        self.send_header("Content-type", "text/html;charset=%s" % enc)
        self.send_header("Content-Length", str(len(errorParametersEncoded)))
        self.end_headers()
        shutil.copyfileobj(f,self.wfile)

# do post Parsing the readline
    def do_POST(self):
        s = str(self.rfile.readline(), 'UTF-8')
        self.send_response(301)
        self.send_header("Location", "/?" + s)
        self.end_headers()
    
class ThreadingHttpServer(ThreadingMixIn, HTTPServer):
    pass
if __name__ == '__main__':
    httpd = ThreadingHttpServer(('10.117.0.217', 9999), ServerForYBS)
    print("server start on ......")
    httpd.serve_forever()

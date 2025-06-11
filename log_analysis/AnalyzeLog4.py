#from korg import LineGrokker, PatternRepo
#import re
import os
import shutil
#import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from processYarnLog import preprocessLog
from readYarnLog import read_yarnlog
from recomendationEngin1 import read_logfile,read_kb,dedupError,findSimilarity,isSimilarityNotFound,recomendSolution
from elastic_utils import elastic_utils 
import email_util as eu
import yaml
import logging



log_file_path=r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\data\la_runlog.log"
logging.basicConfig(filename=log_file_path,level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
   
logging.info('############################ Log Analysis Begin ############################')
def read_cofig(confFile):
    try:
        with open(confFile.split('\\')[-1], 'rb') as ConfigFile:
            confData = yaml.load(ConfigFile)
        return confData
    except FileNotFoundError as err:
        logging.error("AnalyzeLog.read_cofig: Error reading config file: ", err)
        exit()

confs = read_cofig(r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\code\la_configs.yml")

smtpHost=confs["smtp_connection"]["smtp_server"]
smtpPort=confs["smtp_connection"]["port"]


rawFilePath = confs["file_dir_path"]["raw_path"]                     
rawFileName = "1application_1568810042014_225439.log"
logCsvFilePath = confs["file_dir_path"]["csv_path"]                         
logCsvFileName = rawFileName.split(".")[0]+".csv"                              


fullrawFileName=os.path.join(rawFilePath,rawFileName)
fullcsvFileName=os.path.join(logCsvFilePath,logCsvFileName)
#print(fullrawFileName)
#print(fullcsvFileName)
if os.path.exists(fullrawFileName):
    read_yarnlog(fullrawFileName,fullcsvFileName)
else:
    logging.error("AnalyzeLog.read_yarnlog: yarn log is not exist.")
    exit()

####################### Processsed Logs  ######################################

processedFilePth = confs["file_dir_path"]["processed_path"]                     
processedFileName=logCsvFileName
fullprocessedFileName=os.path.join(processedFilePth,processedFileName)
if os.path.exists(fullcsvFileName):
    preprocessLog(fullcsvFileName,fullprocessedFileName)
else:
    logging.error("AnalyzeLog.preprocessLog: file not exist.")
    exit()

####################### Recommendation Engin  #################################

toBeAnalyzed=confs["file_dir_path"]["toBeAnalyzed"]                             

if os.path.exists(fullprocessedFileName):
    logDf=read_logfile(fullprocessedFileName)
else:
    logging.error("AnalyzeLog.read_logfile: file not exist.")
    exit()

dedupLogDf=dedupError(logDf)
    
vectorizer = TfidfVectorizer(binary=False,ngram_range = (1,10),use_idf = True,max_df=0.5,
                             stop_words=['the','for','in','to','but'], smooth_idf=True)

pem_file=confs["elastic_connection"]["pem_file"]
host_name=confs["elastic_connection"]["host_name"]
username=confs["elastic_connection"]["username"]
password=confs["elastic_connection"]["password"]

es=elastic_utils(pem_file,host_name,username,password)
elasticSession=es.connect_elasticsearch()
if elasticSession.ping():
    logging.info('Connection with ElasticSearch server established.')
else:
    logging.info('ElasticSearch connection failed')
    exit()

#print(len(dedupLogDf.index))
if len(dedupLogDf.index)==0:
    #print("No Error Message in Log file: {0}.".format(fullrawFileName))
    eu.send_mail(smtpHost=smtpHost,port=smtpPort,email_conf=confs["test_project"])
    logging.warning('No error message found in log file.')
    exit()
else:
    if len(dedupLogDf.index) >= 1:     
        for err in dedupLogDf["message"]:
            print("----------------------------------------------------------------------") 
            logging.info('processing error message: ',err)
            es_response = es.search_kb(elasticSession,err)
            solutions = es.parse_response(es_response)
            pd.set_option('display.max_colwidth', -1)
            logging.info('solutions: {0}'.format(solutions))
            if len(solutions) < 1:
                print("Error Message: "+ err)
                logging.info('No solution found for error message: ',err)
                es.insert_error(elasticSession,err)
                shutil.copyfile(os.path.join(rawFilePath,rawFileName),os.path.join(toBeAnalyzed,rawFileName))
                logging.info('Error message is added to knowledge base.')
                eu.send_mail(smtpHost=smtpHost,port=smtpPort,email_conf=confs["test_project"],errorMessage=err)
            else:
                #solution=recomendSolution(similarityMatrix,kbDf)        
                print(solutions)
                eu.send_mail(smtpHost=smtpHost,port=smtpPort,email_conf=confs["test_project"],errorMessage=err,solution=solutions)
                logging.info('Solutions emaild to respective team.')
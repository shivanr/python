#from korg import LineGrokker, PatternRepo
#import re
import os
import shutil
#import numpy as np
#import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from processYarnLog import preprocessLog
from readYarnLog import read_yarnlog
from recomendationEngin1 import read_logfile,read_kb,dedupError,findSimilarity,isSimilarityNotFound,recomendSolution
from elastic_utils import elastic_utils 
import email_util as eu
import yaml

def read_cofig(confFile):
    with open(confFile.split('/')[-1], 'rb') as ConfigFile:
        confData = yaml.load(ConfigFile)
    return confData

confs = read_cofig(r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\code\la_configs.yml")

rawFilePath = confs["file_dir_path"]["raw_path"]                                #r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\data\raw_logs"
rawFileName = "1application_1568810042014_225439.log"
logCsvFilePath = confs["file_dir_path"]["csv_path"]                             #r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\data\csv_logs"
logCsvFileName = rawFileName.split(".")[0]+".csv"                               #"1application_1568810042014_225439.csv"


fullrawFileName=os.path.join(rawFilePath,rawFileName)
fullcsvFileName=os.path.join(logCsvFilePath,logCsvFileName)
read_yarnlog(fullrawFileName,fullcsvFileName)

####################### Processsed Logs  ######################################

processedFilePth = confs["file_dir_path"]["processed_path"]                     #r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\data\processted_logs"
processedFileName=logCsvFileName
fullprocessedFileName=os.path.join(processedFilePth,processedFileName)
preprocessLog(fullcsvFileName,fullprocessedFileName)

####################### Recommendation Engin  #################################

#DBFullPath=r'C:\Users\sreddy\OneDrive - MerckGroup\New folder\process_logs\rdb.csv'
toBeAnalyzed=confs["file_dir_path"]["toBeAnalyzed"]                             #r'C:\Users\sreddy\OneDrive - MerckGroup\New folder\data\tobeanalyzed'

#fullprocessedFileName=os.path.join(processedFilePth,processedFileName)

logDf=read_logfile(fullprocessedFileName)
#kbDf=read_kb(DBFullPath)
dedupLogDf=dedupError(logDf)
    
vectorizer = TfidfVectorizer(binary=False,ngram_range = (1,10),use_idf = True,max_df=0.5,
                             stop_words=['the','for','in','to','but'], smooth_idf=True)

#kbVecDf = vectorizer.fit_transform(kbDf['message'])

pem_file=r"C:\installed\deda1x3263_ES-SB.pem"
host_name='deda1x3263.merckgroup.com'
username='aa-nlp'
password='aa-nlp'
es=elastic_utils(pem_file,host_name,username,password)
elasticSession=es.connect_elasticsearch()

print(len(dedupLogDf.index))
if len(dedupLogDf.index)==0:
    print("No Error Message in Log file: {0}.".format(fullrawFileName))
else:
    if len(dedupLogDf.index) >= 1:     
        for err in dedupLogDf["message"]:
            print("------------------------------------------------------------") 
            es_response = es.search_kb(elasticSession,err)
            solutions = es.parse_response(es_response)
            #print(solutions)
            if len(solutions):
            #if isSimilarityNotFound(similarityMatrix):
                print('''No Similarity Found: 
                     Copy file from processed directory to toBeAnalyzed directory
                     categorize error message
                     Notify error message with Classifier findings'''
                )
                print("Error Message: "+ err)
                es.insert_error(elasticSession,err)
                print('cp '+ os.path.join(rawFilePath,rawFileName) +' '+ os.path.join(toBeAnalyzed,rawFileName))
                shutil.copyfile(os.path.join(rawFilePath,rawFileName),os.path.join(toBeAnalyzed,rawFileName))
            else:
                #solution=recomendSolution(similarityMatrix,kbDf)        
                print(solutions)
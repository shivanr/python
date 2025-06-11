#from korg import LineGrokker, PatternRepo
#import re
import os
#import numpy as np
#import pandas as pd
#from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from processYarnLog import preprocessLog
from readYarnLog import read_yarnlog
from recomendationEngin1 import read_logfile,read_kb,dedupError,findSimilarity,isSimilarityNotFound,recomendSolution


rawFilePath = r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\data\raw_logs"
rawFileName = "1application_1568810042014_225439.log"
logCsvFilePath = r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\data\csv_logs"
logCsvFileName = "1application_1568810042014_225439.csv"

fullrawFileName=os.path.join(rawFilePath,rawFileName)
fullcsvFileName=os.path.join(logCsvFilePath,logCsvFileName)
#print(" Raw file Path: "+ fullrawFileName) 
#print(" Log CSV file Path: "+ fullcsvFileName) 
read_yarnlog(fullrawFileName,fullcsvFileName)

####################### Processsed Logs  ######################################

processedFilePth = r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\data\processted_logs"
#processedFileName = "application_1568810042014_190518.csv"
processedFileName=logCsvFileName

fullprocessedFileName=os.path.join(processedFilePth,processedFileName)

preprocessLog(fullcsvFileName,fullprocessedFileName)

####################### Recommendation Engin  #################################


DBFullPath=r'C:\Users\sreddy\OneDrive - MerckGroup\New folder\process_logs\rdb.csv'
toBeAnalyzed=r'C:\Users\sreddy\OneDrive - MerckGroup\New folder\tobeanalyzed'

#fullprocessedFileName=os.path.join(processedFilePth,processedFileName)

logDf=read_logfile(fullprocessedFileName)
kbDf=read_kb(DBFullPath)
dedupLogDf=dedupError(logDf)
    
vectorizer = TfidfVectorizer(binary=False,ngram_range = (1,10),use_idf = True,max_df=0.5,
                             stop_words=['the','for','in','to','but'], smooth_idf=True)

kbVecDf = vectorizer.fit_transform(kbDf['message'])
print(len(dedupLogDf.index))
if len(dedupLogDf.index)==0:
    print("No Error Message in Log file: {0}.".format(fullrawFileName))
else:
    if len(dedupLogDf.index) >= 1:     
        for err in dedupLogDf["message"]:
            print("------------------------------------------------------------") 
            #print(err)            
            #logVecDf = vectorizer.transform([dedupLogDf.iloc[0]["message"]])
            #logVecDf = vectorizer.transform([err]) #commented to use ES
            #similarityMatrix=findSimilarity(logVecDf,kbVecDf) #commented to use ES
            es_response = search_kb(err)
            solutions = parse_response(es_response)
            
            if solutions.empty:
            #if isSimilarityNotFound(similarityMatrix):
                print('''No Similarity Found: 
                     Copy file from processed directory to toBeAnalyzed directory
                     categorize error message
                     Notify error message with Classifier findings'''
                )
                print("Error Message: "+ err)
                os.popen('cp '+ os.path.join(rawFilePath,rawFileName) +' '+ os.path.join(toBeAnalyzed,rawFileName)) 
            else:
                #solution=recomendSolution(similarityMatrix,kbDf)        
                print(solutions)
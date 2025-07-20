from korg import LineGrokker, PatternRepo
import re
import os
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer

def read_yarnlog(InlogFile,outLogFile):
    fullInFileName=InlogFile
    fullOutFileName=outLogFile
    pr = PatternRepo()  # use the std. logstash grok patterns
    outcsv = open(fullOutFileName, "w")
    lg = LineGrokker('%{LOGLEVEL:level} %{SPACE}%{JAVALOGMESSAGE:JavaMessage}%{GREEDYDATA:logdata}', pr)
    lg4 = LineGrokker('%{GREEDYDATA:logdata}', pr)
    pline=[]
    cline={}
    with open(fullInFileName, 'r') as filehandle:
        for line in filehandle:
            cline=lg.grok(line)
            if (cline is not None) and len(pline)==0:         
                if cline['level']=='ERROR':
                    pline.append(cline)
                #elif cline['level']=='er':
                #    cline=lg4.grok(line)
                #    pline.append(cline)
                else:
                    for i in cline.keys():
                        outcsv.write("%s| " % (cline[i]))
                    outcsv.write("\n")
            elif (cline is not None) and len(pline)!=0:         
                if (cline['level']=='er'):
                    cline=lg4.grok(line)
                    pline.append(cline) 
                elif (cline['level']=='INFO'):
                    for indx in pline:
                        for i in indx.keys():
                            if not re.match(r'\s', indx[i]):
                                if re.match(r'^ERROR', indx[i]):
                                    outcsv.write("%s| " % (indx[i]))
                                else:
                                    outcsv.write("%s " % (indx[i]))                                
                    outcsv.write("\n")
                    for i in cline.keys():
                        outcsv.write("%s| " % (cline[i]))
                    outcsv.write("\n")
                    pline=[]                
    #            else:
    #                print(cline)
    #                for i in cline.keys():
    #                    outcsv.write("%s," % (cline[i]))
    #                outcsv.write("2\n") 
            elif (cline is None) and len(pline)!=0:         
                cline=lg4.grok(line)
                pline.append(cline)
    outcsv.close()
    
################################# Pre Processing #########################################


def preprocessLog(inLogFile,outLogFile):
    with open(inLogFile,'r') as read_f:
        lines=read_f.readlines()
    with open(outLogFile,'w') as write_f:
        for line in lines:
            if line.split("|")[0] in ["ERROR","INFO","WARN","FATAL","DEBUG","TRACE"]:
                #SUID Replace
                lowerLine=line.lower()
                #Alias class replace
                modified=re.sub(r'([\w.-]+)@([\w.-]+)',':TOK_ALIAS', lowerLine) 
                modified=re.sub(r' s[0-9._-]+','TOK_SUID', modified)
                #Unix Path replace
                modified=re.sub(r'(/[a-zA-Z0-9\.\-\_/]*/*[\s]?)',' TOK_UNIXPATH ', modified)
                #Unix Path replace
                modified=re.sub(r"[a-z]+.(spark|tcp|https|http:hdfs:)://[a-z]*@+[a-zA-Z0-9\.]*.*",' TOK_URI ', modified)
                #Network IP replace
                modified=re.sub(r'(?<![0-9])(?:(?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2}))(?![0-9])','TOK_IP_ADDRESS', modified)          
                #Host name replace
                #modified=re.sub(r'(?:[A-Za-z0-9\.]{2,63})(?:\.)','TOK_HOSTNAME', modified)           
                #PORT replace
                modified=re.sub(r':\d*\d*\d*\d*\d',':TOK_PORT', modified)
                #All Numbers to o 
                modified=re.sub(r'\d+','0', modified)
                #modified = re.sub('[@#$%^&*;\-_//>\"=\?\>:\\t]+',' ', modified)
                #Remove trailing deliiter
                modified = re.sub('[|]+$','', modified)
                #print(modified)
                write_f.write(modified)
                
                

################################# Find Solution #########################################

def read_kb(dbFullPath):    
    header_list = ["message", "solution"]
    kdbDF=pd.read_csv(dbFullPath,delimiter="|",names=header_list)
    return kdbDF

def read_logfile(fullLogFileName):
    header_list = ["level", "message"]
    log_df=pd.read_csv(fullLogFileName,delimiter="|",names=header_list)
    return log_df

def dedupError(inputDf):
    nodupdf=inputDf.drop_duplicates(subset={"level","message"}, keep='first', inplace=False)
    errordf=nodupdf[nodupdf['level']=='error']
    return errordf

def findSimilarity(inVector1,inVector2):
    result=[]
    result.append(cosine_similarity(inVector1, inVector2))
    print(result)
    return result

def recomendSolution(SimilarityScore,kbDf):
    flat_score = [item for sublist in SimilarityScore for item in sublist]
    score_transposed=np.array(flat_score).T
    similarityDf = pd.DataFrame(score_transposed,columns=["score"])
    kbScoreDf=kbDf.join(similarityDf)
    solutions=kbScoreDf[kbScoreDf['score']>0.8].head(5)[["solution","score"]]
    return solutions

def isSimilarityFound(similarityMatrix):
    flat_score = [item for sublist in similarityMatrix for item in sublist]    
    is_all_zero = np.all((np.array(flat_score) == 0))
    return is_all_zero
                  
if __name__=="__min__":
    rawFilePth = r"C:\your_folder\log_files"
    rawFileName = "1application_1568810042014_190726.log"
    logCsvFilePath = r"C:\your_folder\log_files"
    logCsvFileName = "1application_1568810042014_190726.csv"
    
    fullrawFileName=os.path.join(rawFilePth,rawFileName)
    fullcsvFileName=os.path.join(logCsvFilePath,logCsvFileName)
    
    read_yarnlog(fullrawFileName,fullcsvFileName)
    
    #############################################################################
    processedFilePth = r"C:\your_folder\process_logs"
    processedFileName = "2application_1568810042014_190734_processed.csv"
    
    #fullInLogFileName=os.path.join(logInFilePth,logInFileName)
    fullprocessedFileName=os.path.join(processedFilePth,processedFileName)
    
    preprocessLog(fullcsvFileName,fullprocessedFileName)

    #############################################################################
    DBFullPath=r'C:\your_folder\process_logs\rdb.csv'    
    fullInLogFileName=fullprocessedFileName
    logDf=read_logfile(fullInLogFileName)
    kbDf=read_kb(DBFullPath)
    dedupLogDf=dedupError(logDf)
    vectorizer = TfidfVectorizer(binary=False,max_df=0.95,min_df=0.15,ngram_range = (1,10),use_idf = False, norm = None)
    kbVecDf = vectorizer.fit_transform(kbDf['message'])
    logVecDf = vectorizer.transform([dedupLogDf.iloc[0]["message"]])    
    similarityMatrix=findSimilarity(logVecDf,kbVecDf)    
    if isSimilarityFound:
        print('''No Similarity Found: 
                 Copy file from processed directory to toBeAnalyzed directory
                 categorize error message
                 Notify error message with Classifier findings'''
            )
               
    else:
        solution=recomendSolution(similarityMatrix,kbDf)        
        print(solution)

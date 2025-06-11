import pandas as pd
import re
import numpy as np


def preprocessLog(inLogFile,outLogFile):
#    infile=r'C:\Users\sreddy\OneDrive - MerckGroup\New folder\process_logs\1application_1568810042014_190734.csv'
#    outfile=r'C:\Users\sreddy\OneDrive - MerckGroup\New folder\process_logs\2application_1568810042014_190734_processed.csv'
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
            

#if __name__=="__main__":
#    logInFilePth = r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\process_logs"
#    logInFileName = "1application_1568810042014_190734.csv"
#    logOutFilePth = r"C:\Users\sreddy\OneDrive - MerckGroup\New folder\process_logs"
#    logOutFileName = "2application_1568810042014_190734_processed.csv"
#    
#    fullInLogFileName=os.path.join(logInFilePth,logInFileName)
#    fullOutLogFileName=os.path.join(logOutFilePth,logOutFileName)
#    
#    preprocessLog(fullInLogFileName,fullOutLogFileName)
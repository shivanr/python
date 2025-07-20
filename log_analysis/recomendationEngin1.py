import numpy as np
import os
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer


def read_kb(dbFullPath):    
    header_list = ["message", "solution"]
    kdbDF=pd.read_csv(dbFullPath,delimiter="|",names=header_list)
    return kdbDF

def read_logfile(fullLogFileName):
#    filePath=filePath
#    fileName=fileName
#    fullLogFileName=os.path.join(filePath,fileName)
    fullLogFileName=fullLogFileName
    header_list = ["level", "message","d1","d2"]
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
    solutions=kbScoreDf[kbScoreDf['score']>0.7].head(5)[["solution","score"]]
    if solutions.empty:
        solutions = "No preferred solution found. Please update Knowledge Base with Solution"
    return solutions

def isSimilarityNotFound(similarityMatrix):
    flat_score = [item for sublist in similarityMatrix for item in sublist]  
    is_all_zero = np.all((np.array(flat_score) == 0))
    #print(is_all_zero)
    return is_all_zero
    
def insert_error(elastic_object, index_name, doc):
    is_stored=True
    try:
        outcome = elastic_object.index(index=index_name, doc_type='salads', body=doc)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored

def connect_elasticsearch():
    pem_file=r"C:\installed\deda1x3263_ES-SB.pem"
    context_dev = create_default_context(cafile=pem_file)
    host_name='deda1x3263.merckgroup.com'
    username='aa-nlp'
    password='aa-nlp'
    es = Elasticsearch([host_name],http_auth=(username, password), scheme="https",port=9200, ssl_context=context_dev, timeout=50, max_retries=5, retry_on_timeout=True)
    if es.ping():
        print('Yay Connected')
    else:
        print('Awww it could not connect!')
    return es

def parse_response(response):
    solDF = pd.DataFrame(columns=['err_message', 'solutions', 'score'])
    for items in response["hits"]["hits"]:
        #print(i["_source"]["SOLUTIONS"])
        #print(i["_score"])
        #print(i["_source"]["ERR_MESSAGE"])
        solutionsDF=solDF.append({'err_message' : items["_source"]["ERR_MESSAGE"] , 'solutions' : items["_source"]["SOLUTIONS"], 'score' : items["_score"]} , ignore_index=True)
    return solDF    

def search_kb(err_message):
    search_param = {'query': {'match': {'ERR_MESSAGE': '['+ err_message +']'}}}
    response = es.search(index="solution_base" ,body=search_param)
    return response
 
#if __name__ == "__main__":
#    inputLogPath = r"C:\your_folder\process_logs"
#    inputlogfileName="application_1580556634479_40389_processed.csv"
#    DBFullPath=r'C:\your_folder\process_logs\rdb.csv'
#    toBeAnalyzed=r'C:\your_folder\tobeanalyzed'
#    logDf=read_logfile(inputLogPath,inputlogfileName)
#    kbDf=read_kb(DBFullPath)
#    dedupLogDf=dedupError(logDf)    
#    vectorizer = TfidfVectorizer(binary=False,max_df=0.95,min_df=0.15,
#                                 ngram_range = (1,10),use_idf = False, norm = None)
#    
#    kbVecDf = vectorizer.fit_transform(kbDf['message'])
#    logVecDf = vectorizer.transform([dedupLogDf.iloc[0]["message"]])    
#    similarityMatrix=findSimilarity(logVecDf,kbVecDf)
#    
#    if isSimilarityFound:
#        print('''No Similarity Found: 
#                 Copy file from processed directory to toBeAnalyzed directory
#                 categorize error message
#                 Notify error message with Classifier findings'''
#            )
#        #os.popen('cp '+ os.path.join(inputLogPath,inputlogfileName) +' '+ os.path.join(toBeAnalyzed,inputlogfileName))
#               
#    else:
#        solution=recomendSolution(similarityMatrix,kbDf)        
#        print(solution)
    

        



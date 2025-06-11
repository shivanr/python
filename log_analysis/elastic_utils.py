from elasticsearch import Elasticsearch
from ssl import create_default_context
import pandas as pd 

class elastic_utils:
    def __init__(self,certificate,host,username,passwd):
        self.certificate=certificate
        self.host=host
        self.username=username
        self.passwd=passwd
        self.indexName="solution_base"
    def connect_elasticsearch(self):
        pem_file=self.certificate         #r"C:\installed\deda1x3263_ES-SB.pem"
        context_dev = create_default_context(cafile=pem_file)
        host_name=self.host               #'deda1x3263.merckgroup.com'
        username=self.username            #'aa-nlp'
        password=self.passwd              #'aa-nlp'
        es = Elasticsearch([host_name],http_auth=(username, password), scheme="https",port=9200, ssl_context=context_dev, timeout=50, max_retries=5, retry_on_timeout=True)
        if es.ping():
            print('Connection with ElasticSearch server established.')
        else:
            print('ElasticSearch connection failed')
        return es
    def get_maxId(self,elastic_object,indexName,docType):
        query={"aggs" : {"max_id" : { "max" : { "field" : "id" } }}}
        response=elastic_object.search(index=indexName,doc_type=docType ,body=query)
        max_id=response['hits']['total']['value']
        return max_id
    
    def insert_error(self,elastic_object, doc):
        is_stored=True
        indexName="unresolved_errors"  #"solution_base"
        docType='_doc'
        try:
            max_id=self.get_maxId(elastic_object,indexName,docType)
            newId=max_id+1
            docBody={"ERR_MESSAGE":doc}
            #outcome = elastic_object.index(index=indexName, doc_type=docType,id=newId, body=docBody)
            outcome = elastic_object.index(index=indexName, doc_type=docType, body=docBody)
            print(outcome)
        except Exception as ex:
            print('Error in indexing data')
            print(str(ex))
            is_stored = False
        finally:
            return is_stored
        

    def parse_response(self,response):
        solDF = pd.DataFrame(columns=['err_message', 'solutions', 'score'])
        #print(response)
        for items in response["hits"]["hits"]:
            #print(items["_source"]["SOLUTIONS"])
            #print(items["_score"])
            #print(items["_source"]["ERR_MESSAGE"])
            solDF = solDF.append({'err_message' : items["_source"]["ERR_MESSAGE"] , 'solutions' : items["_source"]["SOLUTIONS"], 'score' : items["_score"]} , ignore_index=True)
        return solDF    
    
    def search_kb(self,elastic_object,err_message):
        search_param = {'query': {'match': {'ERR_MESSAGE': '['+ err_message +']'}}}
        print(search_param)
        response = elastic_object.search(index="solution_base" ,body=search_param)
        print(len(response))
        return response
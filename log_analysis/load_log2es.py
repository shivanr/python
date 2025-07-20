import csv
import urllib2

#https://qbox.io/blog/building-an-elasticsearch-index-with-python

FILE_URL = "http://apps.sloanahrens.com/qbox-blog-resources/kaggle-titanic-data/test.csv"
ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'titanic'
TYPE_NAME = 'passenger'
ID_FIELD = 'passengerid'

response = urllib2.urlopen(FILE_URL)
csv_file_object = csv.reader(response)

header = csv_file_object.next()
header = [item.lower() for item in header]

bulk_data = []
for row in csv_file_object:
    data_dict = {}
    for i in range(len(row)):
        data_dict[header[i]] = row[i]
    op_dict = {
        "index": {
            "_index": INDEX_NAME,
            "_type": TYPE_NAME,
            "_id": data_dict[ID_FIELD]
        }
    }
    bulk_data.append(op_dict)
    bulk_data.append(data_dict)

from elasticsearch import Elasticsearch
# create ES client, create index
es = Elasticsearch(hosts = [ES_HOST])
if es.indices.exists(INDEX_NAME):
    print("deleting '%s' index..." % (INDEX_NAME))
    res = es.indices.delete(index = INDEX_NAME)
    print(" response: '%s'" % (res))
# since we are running locally, use one shard and no replicas
request_body = {
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}
print("creating '%s' index..." % (INDEX_NAME))
res = es.indices.create(index = INDEX_NAME, body = request_body)
print(" response: '%s'" % (res))

print("bulk indexing...")
res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)

res = es.search(index = INDEX_NAME, size=2, body={"query": {"match_all": {}}})
print(" response: '%s'" % (res))

<code>print("results:")
for hit in res['hits']['hits']:
    print(hit["_source"])
</code>
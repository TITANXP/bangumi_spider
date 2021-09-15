import json
import ssl

from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from progress.spinner import Spinner


class MongoElastic:
    def __init__(self, args):
        # load configuration
        self.mongodb_config = args.get('mongodb_config')
        self.es_config = args.get('es_config')

        # batch setting
        self.chunk_size = args.get('chunk_size', 500)
        self.limit = args.get('limit', None)

        # setup mongo client
        self.mongodb_client = MongoClient(
            self.mongodb_config["uri"],
            ssl_cert_reqs=ssl.CERT_NONE
        )

        # setup elasticsearch client
        self.es_client = Elasticsearch(
            hosts=self.es_config["hosts"],
            http_auth=(
                self.es_config["username"],
                self.es_config["password"],
            ),
            scheme=self.es_config["scheme"],
            timeout=40
        )

    def _doc_to_json(self, doc):
        doc_str = json.dumps(doc, default=str)
        doc_json = json.loads(doc_str)
        return doc_json

    def es_add_index_bulk(self, docs):
        actions = []
        for doc in docs:
            doc = self.trans(doc)
            print(doc["id"])
            _id = doc["id"]
            del doc["_id"]
            # print(doc)
            action = {
                "_index": self.es_config["index_name"],
                "_id": _id,
                "_source": doc,
                "_type": self.es_config["type"]
            }
            actions.append(action)

        response = helpers.bulk(self.es_client, actions)
        return response

    def fetch_docs(self, mongodb_query=None, mongodb_fields=None):
        mongodb_query = dict() if not mongodb_query else mongodb_query
        mongodb_fields = dict() if not mongodb_fields else mongodb_fields

        database = self.mongodb_client[self.mongodb_config["database"]]
        collection = database[self.mongodb_config["collection"]]

        no_docs = 0
        offset = 0

        spinner = Spinner('Importing... ')

        while True:
            """
            Iterate to fetch documents in batch.
            Iteration stops once it hits limit or no document left.
            """
            mongo_cursor = collection.find(mongodb_query, mongodb_fields)
            mongo_cursor.skip(offset)
            mongo_cursor.limit(self.chunk_size)
            docs = list(mongo_cursor)
            # break loop if no more documents found
            if not len(docs):
                break
            # convert document to json to avoid SerializationError
            docs = [self._doc_to_json(doc) for doc in docs]
            yield docs
            # check for number of documents limit, stop if exceed
            no_docs += len(docs)
            if self.limit and no_docs >= self.limit:
                print(self.limit," and ", no_docs, ' >= ', self.limit)
                break
            # update offset to fetch next chunk/page
            offset += self.chunk_size
            spinner.next()
        print('no_doc:',no_docs)

        self.mongodb_client.close()

    def start(self, mongodb_query=None, mongodb_fields=None):
        self.create_index(self.es_config['index_name'])
        for docs in self.fetch_docs(mongodb_query, mongodb_fields):
            self.es_add_index_bulk(docs)

    # 0000-00-00直接插入ES会报错
    def trans(self, doc):
        # if doc['api']['air_date'] == '0000-00-00':
        #     doc['api']['air_date'] = '1970-01-01' # ES date类型最小为1970-01-01
        # elif doc['api']['air_date'] == '每天':
        #     doc['api']['air_date'] = '1970-01-01'
        # if '放送日期' in doc['info']:
        #     if doc['info']['放送日期'] == '0000-00-00':
        #         doc['api']['放送日期'] = '1970-01-01' # ES date类型最小为1970-01-01
        #     elif doc['info']['放送日期'] == '每天':
        #         doc['info']['放送日期'] = '1970-01-01'
        return doc

    def create_index(self, index_name):
        """
        创建index
        :param index_name:
        :return:
        """
        self.es_client.indices.create(
            index=index_name,
            body={
                'settings':{'index':{'mapping':{'total_fields':{'limit':20000}}}},
                # 'mappings':{
                #     # "date_detection": False
                #     "dynamic_date_formats": ["MM-dd-yyyy HH:mm"]
                # }
            }
        ) # 因为字段数过多，所以事先创建索引，并指定最多字段数
        # self.es_client.update(index=index_name,doc_type='_setting', body={"index.mapping.total_fields.limit": 2000})
        self.es_client.indices.put_mapping(index=index_name, body={
                    "date_detection": False
                })
        # self.es_client.indices.put_mapping(index=index_name, body={ "date_detection": False,"dynamic":True})


if __name__ == '__main__':
    config = {
        "mongodb_config": {
            # check more for mongo uri here - https://docs.mongodb.com/manual/reference/connection-string/
            "uri": 'mongodb://localhost:27017',
            "database": "bangumi_test",
            "collection": "anime1"
        },
        "es_config": {
            "hosts": ["127.0.0.1:9200"],
            "username": "elastic",
            "password": "changeme",
            "index_name": "bangumi_anime",
            "type": "_doc",
            "scheme": "http"
        },
        'chunk_size': 100,
        # Set limit=None to push all documents matched by the query
        'limit': None
    }
    obj = MongoElastic(config)
    mongodb_query = {}
    # mongodb_fields = {"title": 1, "description": 1}
    # TODO 去掉alias（别名），因为会插入ES时会报错： object mapping for [api.crt.info.alias] tried to parse field [null] as object, but found a concrete value
    mongodb_fields = {
        # "api.staff.info.alias":0,
        # "api.staff.info.毕业院校": 0,
        "api.staff.info": 0,
        # "api.crt.info.alias":0,
        # "api.crt.info.weight": 0,
        # "api.crt.info.喜欢": 0,
        # "api.crt.info.height": 0,
        # "api.crt.info.bwh": 0,
        "api.crt.info": 0,
        # "tags": 0
    }
    obj.start(mongodb_query=mongodb_query, mongodb_fields=mongodb_fields)
    # obj.start(mongodb_query=mongodb_query)
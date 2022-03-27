import pymongo

import parse_process.parse as parse
from input_preprocess.XpathQuery_preprocess import data_preprocess


def ApplyMongoDBQuery(database, collection, xpathQuery, isFullPath):
    database = 'test'
    collection = 'library'

    preprocess_result=[]
    parse_result = []

    # todo:连接数据库
    client = pymongo.MongoClient("localhost", 27017)
    # todo：选择数据库
    mydb = client[database]
    # todo：选择collection
    mycol = mydb[collection]


    # todo: input_preprocess 的 逻辑

    preprocess_result = data_preprocess(xpathQuery,isFullPath,database,collection)

    # todo：parse的逻辑

    for preprocessed_xpathQuery in preprocess_result:
        filter = parse.parse_to_MongoDB_Query_filter(preprocessed_xpathQuery)
        projection = parse.parse_to_MongoDB_Query_projection(preprocessed_xpathQuery)
        result = mycol.find(filter, projection)
        parse_result.append(result)





    # todo: output_preprocess 的 逻辑

    input

    return

if __name__ == '__main__':
    XpathQuery = "child::library/child::album[child::year>=1990 or child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name"

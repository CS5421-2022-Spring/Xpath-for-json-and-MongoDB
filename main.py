import pymongo

import parse_process.parse as parse
from input_preprocess.XpathQuery_preprocess import data_preprocess
from output_preprocess.handle_output import finalOutput


def ApplyMongoDBQuery(database, collection, xpathQuery, isFullPath):
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

    for each_preprocessed_result in preprocess_result:
        (preprocessed_xpathQuery,operator) = each_preprocessed_result
        filter = parse.parse_to_MongoDB_Query_filter("",preprocessed_xpathQuery)
        projection = parse.parse_to_MongoDB_Query_projection(preprocessed_xpathQuery)
        result = mycol.find(filter, projection)

        parse_result.append((result,operator,projection))
        # finalResult=finalOutput(result,projection,operator)
        # print(finalResult)






    # todo: output_preprocess 的 逻辑

    for each_parsed_result in parse_result:
        (result, operator, projection) = each_parsed_result
        finalResult = finalOutput(result, projection, operator)
        print(finalResult)


    return parse_result

if __name__ == '__main__':
    # XpathQuery = "child::library/child::album[child::year>=1990 or child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name"
    XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name/child::text()"
    # XpathQuery = "count(child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name)"
    result = ApplyMongoDBQuery('test','library',XpathQuery,True)

    # x=1+1

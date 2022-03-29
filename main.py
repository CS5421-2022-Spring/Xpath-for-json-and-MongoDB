import pymongo
import parse_process.parse as parse
from input_preprocess.XpathQuery_preprocess import data_preprocess
from output_preprocess.handle_output import finalOutput

def ApplyMongoDBQuery(database, collection, xpathQuery, isFullSyntax):
    parse_result = []
    final_result = []

    # todo:连接数据库
    client = pymongo.MongoClient("localhost", 27017)
    # todo：选择数据库
    mydb = client[database]
    # todo：选择collection
    mycol = mydb[collection]

    # todo: input_preprocess 的 逻辑
    preprocess_result = data_preprocess(xpathQuery,isFullSyntax,database,collection)

    # todo：parse的逻辑
    for each_preprocessed_result in preprocess_result:
        (preprocessed_xpathQuery,operator) = each_preprocessed_result
        filter = parse.parse_to_MongoDB_Query_filter("",preprocessed_xpathQuery)
        projection = parse.parse_to_MongoDB_Query_projection(preprocessed_xpathQuery)
        result = mycol.find(filter, projection)
        parse_result.append((result,operator,projection))

    # todo: output_preprocess 的 逻辑

    for each_parsed_result in parse_result:
        (result, operator, projection) = each_parsed_result
        finalResult = finalOutput(result, projection, operator)
        final_result.append(finalResult)
        print(finalResult)

    return final_result

if __name__ == '__main__':
    isFullSyntax = True

    # todo:未通过测试

    # todo:通过测试
    # ----------------------------or------------------------------
    XpathQuery = "child::library/child::album[child::year>=1990 or child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name"


    # ----------------------------and----------------------------
    # XpathQuery = "child::library/child::album[child::year>=1990 and child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name"


    # --------------------------count()--------------------------
    # 加上count前
    # XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name"
    # 加上count()后
    # XpathQuery = "count(child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name)"


    # --------------------------text()---------------------------
    # 加上text()前
    # XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name"
    # 加上text()后
    # XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name/child::text()"


    # --------------shorthand(isFullSyntax = False)--------------
    # 记得 设置 isFullSyntax = False
    # isFullSyntax = False
    # XpathQuery = "library/album[year>=1990 or year<=2000]/artists/artist[country='Indonesia']/name"
    # XpathQuery = "library/album[year>=1990 and year<=2000]/artists/artist[country='Indonesia']/name"


    #---------------shorthand && descendant("//") ---------------
    # isFullSyntax = False
    # XpathQuery = "library/album[year>=1990 and year<=2000]//artist[country='Indonesia']/name"


    # ------------------------descendant-------------------------
    # XpathQuery = "descendant::album[child::year>=1990 or child::year<=2000]/descendant::artist[child::country='Indonesia']/child::name"
    # XpathQuery = "descendant::title"

    result = ApplyMongoDBQuery('test','library',XpathQuery,isFullSyntax)


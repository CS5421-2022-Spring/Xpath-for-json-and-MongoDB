
def handleCount(projection,result):
    keys = list(projection.keys())[0]
    first_key = keys.split('.')[0]
    last_key = keys.split('.')[-1]

    # count the number of nodes in the final result
    def count(last_key, dictionary):
        # Each dictionary only has one key and one value except the first dictionary.
        value = list(dictionary.values())[0]
        key = list(dictionary.keys())[0]
        # Reach to the last nodes.
        if key == last_key:
            if isinstance(value, list):
                return len(value)
            else:
                return 1
        # The corresponding value is a dictionary, search this dictionary
        if isinstance(value, dict):
            return count(last_key, value)
        # The corresponding value is a list, for each dictionary in the list, search the dictionary.
        # And sum up the result.
        if isinstance(value, list):
            sum = 0
            for dic in value:
                sum  = sum + count(last_key,dic)
            return sum

    count_result = 0
    for doc in result:
        #temp = doc[keys.split('.')[0]]
        temp = doc[first_key]
        number = count(last_key,temp)
        count_result = count_result + number

    return count_result

def handleText(projection,result):
    keys = list(projection.keys())[0]
    first_key = keys.split('.')[0]
    last_key = keys.split('.')[-1]

    # Directly print the text node if the query result is text node. If not, return NONE.
    def text(last_key, dictionary):
        value = list(dictionary.values())[0]
        key = list(dictionary.keys())[0]
        if key == last_key:
            if isinstance(value, list) and isinstance(value[0], str):
                return value
            elif isinstance(value, dict) or isinstance(value, list):
                return
            # elif isinstance(value, list) and isinstance(value[0], str):
            #     return value
            else:
                return value
        if isinstance(value, dict):
            return text(last_key, value)
        if isinstance(value, list):
            lst = []
            for dic in value:
                lst.append(text(last_key,dic))
            return lst
        return

    text_result = []

    for doc in result:
        #temp = doc[keys.split('.')[0]]
        temp = doc[first_key]
        temp_result = text(last_key,temp)
        text_result = text_result + temp_result

    return text_result


if __name__=="__main__":
    import pymongo
    from parse import *
    from bson.json_util import dumps

    database = 'test'
    collection = 'library'

    # # todo:连接数据库
    # client = pymongo.MongoClient("localhost", 27017)
    # # todo：选择数据库
    # mydb = client[database]
    # # todo：选择collection
    # mycol = mydb[collection]
    # #result = mycol.find()
    # filter = {'library.album.artists.artist.name':'Anang Ashanty'}
    # #projection = {'library.album.artists.artist.name': 'Anang Ashanty'}
    # #projection = {'library.album.songs.song':1}
    # projection = {'library.album.songs.song.title':1}
    # result = mycol.find(filter, projection)
    # keys = list(projection.keys())[0]
    # first_key = keys.split('.')[0]
    # last_key = keys.split('.')[-1]

    # XpathQuery = "count(child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::songs/child::song/child::title)"
    # XpathQuery = "count(child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::genres/child::genre)"
    # XpathQuery = "child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::songs/child::song/child::title/text()"
    XpathQuery = "child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::genres/child::genre/text()"

    (sanitized_query, function) = data_preprocess(XpathQuery)
    filter = parse_to_MongoDB_Query_filter("", sanitized_query)
    projection = parse_to_MongoDB_Query_projection(sanitized_query)
    cursor = ApplyMongoDBQuery(database, collection, filter, projection, function)
    result = eval(dumps(list(cursor)))
    # print(handleCount(projection,result))
    # print(projection)
    # print(filter)
    # print(result)
    print(handleText(projection,result))

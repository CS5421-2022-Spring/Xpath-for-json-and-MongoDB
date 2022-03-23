import re
import pymongo

# todo:初始化
split_pattern = re.compile('\/')
split_index = (0, 1)
Mongo_filter = {}
Mongo_projection = {}
Mongo_path = ''
parse_complete_flag = False


# todo：判断path是否可以被截断
def canSplit(path):
    parse_stack = []
    for char in path:
        if (char == '['):
            parse_stack.append('[')
        if (char == ']'):
            parse_stack.pop()
    if len(parse_stack) != 0:
        return False
    else:
        return True


def parse_to_MongoDB_Query_projection(XpathQuery):
    pattern_axis_child_path = re.compile('^child\:\:[a-z]*')
    match_result = pattern_axis_child_path.match(XpathQuery)
    match_index = match_result.span()
    new_step_path = XpathQuery[7:match_index[1]]
    return new_step_path


def parse_to_MongoDB_Query_filter(MongoDB_previous_path, XpathQuery):
    # XpathQuery = "child::library/child::album[child::artists/child::artist[child::name='Anang Ashanty'] and child::artists/child::artist[child::country='Indonesia']]/title"
    # XpathQuery = "child::library/child::album[child::year>=1990 and child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name"

    MongoDB_Query_filter = {}
    parse_complete_flag = False
    split_index = [0, 1]
    split_pattern = re.compile('\/')

    # todo:从左到右解析 Xpath语句 更新 Mongo_filter
    while (parse_complete_flag == False):
        # todo:查找下一个 '/'

        search_result = split_pattern.search(XpathQuery, split_index[1])
        # todo: 递归退出条件，XpathQuery 中不含 /
        if (search_result == None):
            return None

        split_index = search_result.span()

        # todo:判断当前的 / 是否可以被split
        split_XpathQuery = XpathQuery[0:split_index[1]]
        # todo:如果可以被截断
        if (canSplit(split_XpathQuery)):
            # todo: 更新 Mongo_filter
            new_filters = generate_MongoDB_filters(MongoDB_previous_path, split_XpathQuery)
            MongoDB_Query_filter.update(new_filters)

            # todo：更新 MongoDB_previous_path
            if (MongoDB_previous_path == ""):
                MongoDB_previous_path = generate_MongoDB_path(split_XpathQuery)
            else:
                MongoDB_previous_path = MongoDB_previous_path + '.' + generate_MongoDB_path(split_XpathQuery)

            # todo:更新 XpathQuery
            XpathQuery = XpathQuery[split_index[1]:]

            # todo:退出while 循环
            parse_complete_flag = True

        # todo:如果不能被截断,进入下一次while循环，找下一个可以被截断的'/'
        else:
            continue

    following_path_filters = parse_to_MongoDB_Query_filter(MongoDB_previous_path, XpathQuery)
    if (following_path_filters != None):
        MongoDB_Query_filter.update(following_path_filters)

    # todo:更新 Mongo_projection
    return MongoDB_Query_filter.copy()


def generate_MongoDB_filters(MongoDB_previous_path, split_XpathQuery):
    filters = {}
    # todo: 需要的 pattern:
    pattern_greater_or_equal_than = re.compile('\>\=')
    pattern_greater_than = re.compile('\>')
    pattern_less_or_equal_than = re.compile('\<\=')
    pattern_less_than = re.compile('\<')
    pattern_equal_than = re.compile('\=')

    pattern_String_value = re.compile('^\'[A-Za-z\s]+\'$')

    # todo：先判断是否有 predicate
    pattern_predicate = re.compile('\[.*\]\/$')
    search_result = pattern_predicate.search(split_XpathQuery)
    if (search_result == None):
        # 如股没有predicate 返回空 {}
        return filters
    else:
        # todo:匹配 child::
        pattern_axis_child_path = re.compile('^child\:\:[a-z]*')
        match_result = pattern_axis_child_path.match(split_XpathQuery).span()
        generate_path = split_XpathQuery[7:match_result[1]]

        if (MongoDB_previous_path == ""):
            MongoDB_previous_path = generate_path
        else:
            MongoDB_previous_path = MongoDB_previous_path + '.' + generate_path
        predicate = split_XpathQuery[match_result[1] + 1:len(split_XpathQuery) - 2]

        pattern_predicate_2 = re.compile('\[.*\]$')
        search_result = pattern_predicate_2.search(predicate)
        if (search_result == None):
            # todo:condition 1：predicate 里面 没有predicate

            # todo:如果有 "and" predicate 需要拆分成小的 predicate_atom
            predicate_split_result = predicate.split(sep=' and ')

            # todo：解析每个 predicate_atom 的语义
            for predicate_atom in predicate_split_result:
                # todo: 解析 axis
                # 还需要考虑其他axis的情况
                # axis 为 child的情况
                pattern_axis_child_tagname_slash = re.compile('^child\:\:[a-zA-Z]+\/')
                pattern_axis_child = re.compile('^child\:\:')
                temp_prediction = predicate_atom
                temp_path = MongoDB_previous_path

                #todo: 要考虑 多级路径的情况 例如：child::artist/child::name='Anang Ashanty'
                match_result = pattern_axis_child_tagname_slash.match(temp_prediction)
                while(match_result != None):
                    if (match_result != None):
                        temp_path = temp_path + '.' + temp_prediction[7:match_result.span()[1] - 1]
                        temp_prediction = temp_prediction[match_result.span()[1]:]
                    match_result = pattern_axis_child_tagname_slash.match(temp_prediction)

                match_result = pattern_axis_child.match(temp_prediction)
                temp_prediction = temp_prediction[match_result.span()[1]:]


                # todo：解析 内容
                # todo: >=
                search_result = pattern_greater_or_equal_than.search(temp_prediction)
                if (search_result != None):
                    key = temp_path + "." + temp_prediction[0:search_result.span()[0]]
                    value = int(temp_prediction[search_result.span()[1]:])
                    if (key in filters):
                        previous_value = filters.pop(key)
                        previous_value.setdefault("$gte", value)
                        filters.setdefault(key, previous_value)
                    else:
                        filters.setdefault(key, {"$gte": value})
                    continue

                # todo:>
                search_result = pattern_greater_than.search(temp_prediction)
                if (search_result != None):
                    key = temp_path + "." + temp_prediction[0:search_result.span()[0]]
                    value = int(temp_prediction[search_result.span()[1]:])
                    if (key in filters):
                        previous_value = filters.pop(key)
                        previous_value.setdefault("$gt", value)
                        filters.setdefault(key, previous_value)
                    else:
                        filters.setdefault(key, {"$gt": value})
                    continue

                # todo:<=
                search_result = pattern_less_or_equal_than.search(temp_prediction)
                if (search_result != None):
                    key = temp_path + "." + temp_prediction[0:search_result.span()[0]]
                    value = int(temp_prediction[search_result.span()[1]:])
                    if (key in filters):
                        previous_value = filters.pop(key)
                        previous_value.setdefault("$lte", value)
                        filters.setdefault(key, previous_value)
                    else:
                        filters.setdefault(key, {"$lte": value})
                    continue

                search_result = pattern_less_than.search(temp_prediction)
                # todo:<
                if (search_result != None):
                    key = temp_path + "." + temp_prediction[0:search_result.span()[0]]
                    value = int(temp_prediction[search_result.span()[1]:])
                    if (key in filters):
                        previous_value = filters.pop(key)
                        previous_value.setdefault("$lt", value)
                        filters.setdefault(key, previous_value)
                    else:
                        filters.setdefault(key, {"$lt": value})
                    continue

                # todo:=
                search_result = pattern_equal_than.search(temp_prediction)
                if (search_result != None):
                    key = temp_path + "." + temp_prediction[0:search_result.span()[0]]
                    value = temp_prediction[search_result.span()[1]:]
                    # todo:判断是 int 还是 string
                    match_result = pattern_String_value.match(value)
                    if (match_result == None):
                        value = int(value)
                    else:
                        value = value[1:len(value) - 1]
                    filters.setdefault(key, value)
                    continue
        else:
            # todo:condition 2 predicate 里面 还有predicate
            sub_filters = parse_to_MongoDB_Query_filter(MongoDB_previous_path, predicate+'/')
            filters.update(sub_filters)

        return filters.copy()


def generate_MongoDB_path(split_XpathQuery):
    # todo:匹配 child::
    pattern_axis_child = re.compile('^child\:\:[a-z]*')
    match_result = pattern_axis_child.match(split_XpathQuery).span()
    generate_path = split_XpathQuery[7:match_result[1]]

    # todo: 需要考虑其他axis

    return generate_path


# def XpathQuery_to_MongoDBQuery(XpathQuery):
#     # todo:输入Xpath查询语句
#     XpathQuery = "doc('library.xml')/child::library/child::album[child::artists/child::artist[child::name='Anang Ashanty'] and child::artists/child::artist[child::country='Indonesia']]/title"
#
#     # todo:得到 mongoDB对应的collection名
#
#     return


def ApplyMongoDBQuery(database, collection, filter, projection):
    database = 'test'
    collection = 'library'

    # todo:连接数据库
    client = pymongo.MongoClient("localhost", 27017)
    # todo：选择数据库
    mydb = client[database]
    # todo：选择collection
    mycol = mydb[collection]

    result = mycol.find(filter, projection)

    return result


if __name__ == '__main__':
    # test function
    # temp = generate_MongoDB_filters("library", "child::album[child::year>=1990 and child::year<=2000]/")
    # temp = generate_MongoDB_filters("library", "child::album[child::artists[child::artist/child::name='Anang Ashanty']/child::artist[country='Indonesia']]/")
    # temp = generate_MongoDB_filters("library.album", "child::artists[child::artist/child::name='Anang Ashanty']/")

    # todo:输入Xpath查询语句 开头加上 doc('library.xml')/
    # XpathQuery = "child::library/child::album[child::artists/child::artist[child::name='Anang Ashanty'] and child::artists/child::artist[child::country='Indonesia']]/title"
    # XpathQuery = """
    #               child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']/child::artist[country = "Indonesia"]]/child::artist[country = 'Indonesia']/name
    #              """
    XpathQuery = "child::library/child::album[child::year>=1990 and child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name"
    XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/name"

    # todo:对 XpathQuery 字符串预处理
    # 去掉空格
    # strip两头

    # todo:解析 database name 和 collection name
    database = 'test'
    collection = 'library'

    # todo: XpathQuery 转 MongoDBQuery
    filter = parse_to_MongoDB_Query_filter("", XpathQuery)
    projection = parse_to_MongoDB_Query_projection(XpathQuery)

    # todo MongoDBQuery
    result = ApplyMongoDBQuery(database, collection, filter, projection)

    # pattern1 = re.compile('\[.*\]')
    # pattern2 = re.compile('^child\:\:[a-z]*')
    # result = pattern2.search("child::artist[child::country='Indonesia']")
    # print(result)
    #
    # pattern3 = re.compile('^child\:\:[a-z]*\[.*\]')
    # pattern = re.compile('\/')

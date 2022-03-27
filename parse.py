import re
import pymongo


# todo：判断path是否可以被截断
def canSplit(path):
    """
    :param path: 需要判断的path
    :return: 返回 True 或 False
    """
    parse_stack = []
    for char in path:
        if (char == '['):
            parse_stack.append('[')
        if (char == ']'):
            parse_stack.pop()
    # 通过判断 [] 是否匹配 判断 path是否可以是split之后的
    if len(parse_stack) != 0:
        return False
    else:
        return True


# todo: 根据 XpathQuery 获得 projection
def parse_to_MongoDB_Query_projection(XpathQuery):
    """

    :param XpathQuery: 形为 "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name"的查询语句
    :return: 基于上述 Xpath Query 产生的 projection
    """
    parse_complete_flag = False
    MongoDB_Query_projection = {}
    MongoDB_Query_projection_path = ""
    # 这里split_index 写成这种形式 主要是为了 与正则表达式 "返回结果.span()"格式统一
    split_index = [0, 1]
    split_pattern = re.compile('\/')

    # todo:从左到右解析 Xpath语句
    while (parse_complete_flag == False):
        # todo: 查找下一个 以'/'结尾的 子path
        search_result = split_pattern.search(XpathQuery, split_index[1])

        if (search_result == None):
            # todo: 循环退出条件，XpathQuery 中不含 "/" ；原说明到结尾了
            # 例如 'child::name'

            # todo：更新 MongoDB_Query_projection_path
            if (MongoDB_Query_projection_path == ""):
                MongoDB_Query_projection_path = generate_MongoDB_path(XpathQuery)
            else:
                MongoDB_Query_projection_path = MongoDB_Query_projection_path + '.' + generate_MongoDB_path(XpathQuery)

            parse_complete_flag = True
        else:
            split_index = search_result.span()
            # todo:判断当前的 以'/'结尾的 path 是否可以被split
            split_XpathQuery = XpathQuery[0:split_index[1]]
            # todo:如果可以被截断
            if (canSplit(split_XpathQuery)):

                # todo：更新 MongoDB_Query_projection_path
                if (MongoDB_Query_projection_path == ""):
                    MongoDB_Query_projection_path = generate_MongoDB_path(split_XpathQuery)
                else:
                    MongoDB_Query_projection_path = MongoDB_Query_projection_path + '.' + generate_MongoDB_path(
                        split_XpathQuery)

                # todo:更新 XpathQuery 并 重置 split_index
                XpathQuery = XpathQuery[split_index[1]:]
                split_index = [0, 1]
            else:
                # todo: 当 当前 '/' 位置 不可分割的时候，不更新XpathQuery， 不重置 split_index，直接进入下一次while循环
                continue

    MongoDB_Query_projection.setdefault(MongoDB_Query_projection_path, 1)
    return MongoDB_Query_projection


# todo: 根据 XpathQuery 获得 filter
def parse_to_MongoDB_Query_filter(MongoDB_previous_path, XpathQuery):
    """

    :param MongoDB_previous_path: 已解析过的父路径（初始调用 应该传入 空字符串""）
    :param XpathQuery: 待解析的 XpathQuery
    :return: 对应的filter

    注意： 这是解析 "初始 Xpath query 语句" 所对应的 filter 的入口函数；
    同时此函数内部会递归调用 parse_to_MongoDB_Query_filter自己
    此函数每切割一个子path后， 会产生产生split_XpathQuery，会调用 generate_MongoDB_filters，得到 split_XpathQuery对应的 filter
    """
    MongoDB_Query_filter = {}
    parse_complete_flag = False
    # 这里split_index 写成这种形式 主要是为了 与正则表达式 "返回结果.span()"格式统一
    split_index = [0, 1]
    split_pattern = re.compile('\/')

    # todo:从左到右解析 Xpath语句 更新 Mongo_filter
    while (parse_complete_flag == False):
        # todo:查找下一个 以'/'结尾的 子path

        search_result = split_pattern.search(XpathQuery, split_index[1])
        # todo: 递归退出条件，XpathQuery 中不含 /
        if (search_result == None):
            # 这里 search_result == None 是因为 解析到了 最后一个 path
            # 例如 "child::name[child::age>0]"
            # 并不是 以 '/结尾'，为了减少代码量 统一处理 直接 结尾 加 '/'
            XpathQuery = XpathQuery + '/'
            search_result = split_pattern.search(XpathQuery, split_index[1])

        split_index = search_result.span()

        # todo:判断当前的 以'/'结尾的 path 是否可以被split
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

        # todo:如果当前位置'/'不能被截断, 则进入下一次while循环，找下一个可以被截断的'/'
        else:
            continue

    # XpathQuery 为 ""时，说明已经扫描到了结尾
    if (XpathQuery != ''):
        following_path_filters = parse_to_MongoDB_Query_filter(MongoDB_previous_path, XpathQuery)
        MongoDB_Query_filter.update(following_path_filters)

    # todo:更新 Mongo_projection
    return MongoDB_Query_filter.copy()


# todo: 根据 parse_to_MongoDB_Query_filter方法，切割后产生的 split_XpathQuery 返回对应filter
def generate_MongoDB_filters(MongoDB_previous_path, split_XpathQuery):
    """

    :param MongoDB_previous_path: parse_to_MongoDB_Query_filter方法， 切割后产生的 MongoDB_previous_path
    :param split_XpathQuery: parse_to_MongoDB_Query_filter方法，切割后产生的 split_XpathQuery
    :return: 对应的filter
    注意：此方法还会调用 parse_to_MongoDB_Query_filter方法， 因为 "predicate里有predicate"的情况
    """
    filters = {}
    # todo: 正则表达式需要的 pattern:
    pattern_and = re.compile('\sand\s')

    # todo：先判断是否有 predicate
    pattern_predicate = re.compile('\[.*\]\/$')  # 注意这里匹配的时候 要求以']'紧接着'/'结尾，以达到"]"是最后一个的要求，否则嵌套的[]会对后面代码产生错误
    search_result = pattern_predicate.search(split_XpathQuery)
    if (search_result == None):
        # 如股没有predicate 返回空 {}
        return filters
    else:
        # todo:匹配 axis child::
        pattern_axis_child_path = re.compile(
            '^child\:\:[a-z]*')  # 例如： child::artist[child::name='Anang Ashanty']/ 返回匹配结果 child::artist
        match_result = pattern_axis_child_path.match(split_XpathQuery).span()
        generate_path = split_XpathQuery[7:match_result[1]]  # 读出紧跟在 child::后面的 path 名称

        if (MongoDB_previous_path == ""):
            MongoDB_previous_path = generate_path
        else:
            MongoDB_previous_path = MongoDB_previous_path + '.' + generate_path

        predicate = split_XpathQuery[
                    match_result[1] + 1:len(split_XpathQuery) - 2]  # 去掉axis 和 "[]/", 读出[predicate]里面的内容 predicate

        # todo：这里考虑 [] 里边嵌套 []的情况，即 predicate 里面有predicate
        pattern_predicate_2 = re.compile('\[.*\]')
        search_result = pattern_predicate_2.search(predicate)
        if (search_result == None):
            # todo:condition 1：predicate 里面 没有predicate
            temp_filters = parse_predicate(MongoDB_previous_path, predicate)
            filters.update(temp_filters)

        else:
            # todo:condition 2 predicate 里面 还有predicate 把 刚刚那层 []去掉之后 发现现在里边还有[]
            # 极端情况 里边还有 "and"
            # 例如去掉外层[] 之后 predicate为：
            # child::artists/child::artist[child::age>=20 and child::age<=30] and child::artists/child::artist[child::country='Indonesia']
            # 或
            # child::artists/child::artist/child::name='Anang Ashanty' and child::artists/child::artist[child::country='Indonesia']
            # 或
            #

            search_result = pattern_and.search(predicate)
            if (search_result == None):
                # todo: condition 2.1 predicate 里面 含有 "[]", 不含有 "and"
                sub_filters = parse_to_MongoDB_Query_filter(MongoDB_previous_path, predicate + '/')
                filters.update(sub_filters)
            else:
                # todo: condition 2.2 predicate 里面 含有 "[]", 而且含有 "and"
                # todo: 需要从左到右 挨个看当前 "and" 是否能切割
                split_index = [0, 0]
                while True:
                    # todo:查找下一个 可以被切割的 "and"
                    search_result = pattern_and.search(predicate, split_index[1])
                    if (search_result == None):
                        # todo：扫描到了predicate的结尾
                        # todo：需要判断 predicate 是否含有 "[]"
                        pattern_predicate_3 = re.compile('\[.*\]')
                        search_result = pattern_predicate_3.search(predicate)
                        if (search_result == None):
                            # todo:predicate 不含 "[]"
                            temp_filters = parse_predicate(MongoDB_previous_path, predicate)
                            filters.update(temp_filters)
                        else:
                            # todo:predicate 含 "[]"
                            sub_filters = parse_to_MongoDB_Query_filter(MongoDB_previous_path, predicate + '/')
                            filters.update(sub_filters)
                        break

                    split_index = search_result.span()
                    split_predicate = predicate[0:split_index[0]]
                    if (canSplit(split_predicate)):
                        # todo：找到了 可以被切割的 "and"
                        # todo:需要判断 and 前被切割下来的 split_predicate 是否含有 "[]"

                        pattern_predicate_3 = re.compile('\[.*\]')
                        search_result = pattern_predicate_3.search(split_predicate)
                        if (search_result == None):
                            # todo:被切割下来的 "and" 前的 predicate 不含 "[]"
                            temp_filters = parse_predicate(MongoDB_previous_path, split_predicate)
                            filters.update(temp_filters)
                        else:
                            # todo:被切割下来的 "and" 前的 predicate 含 "[]"

                            sub_filters = parse_to_MongoDB_Query_filter(MongoDB_previous_path, split_predicate + '/')
                            filters.update(sub_filters)

                        # todo:更新 predicate
                        predicate = predicate[split_index[1]:]

                        continue
                    else:
                        # todo：这个 "and" 不可以被切割
                        continue
        return filters.copy()


def parse_predicate(MongoDB_previous_path, predicate):
    # todo：用于 and 连接的 条件
    filters = {}
    # todo：用于 or 连接的 条件
    filters_or = []

    # todo: 正则表达式需要的 pattern:
    pattern_greater_or_equal_than = re.compile('\>\=')
    pattern_greater_than = re.compile('\>')
    pattern_less_or_equal_than = re.compile('\<\=')
    pattern_less_than = re.compile('\<')
    pattern_equal = re.compile('\=')
    pattern_not_equal = re.compile('\!\=')

    pattern_String_value = re.compile('^\'[A-Za-z\s]+\'$')

    pattern_and = re.compile('\sand\s')
    pattern_or = re.compile('\sor\s')

    search_result_and = pattern_and.search(predicate)
    search_result_or = pattern_or.search(predicate)

    if ((search_result_and != None) | (search_result_or == None)):
        # todo:如果有 "and" predicate 需要拆分成小的 predicate_atom
        # 例如 child::year>=1990 and child::year<=2000
        predicate_split_result = predicate.split(sep=' and ')

        # todo：解析每个 predicate_atom 的语义
        for predicate_atom in predicate_split_result:
            # todo: 解析 axis
            # todo：还需要考虑其他axis的情况？

            # todo：axis 为 child的情况
            pattern_axis_child_tagname_slash = re.compile('^child\:\:[a-zA-Z]+\/')  # 匹配诸如 child::artist/
            pattern_axis_child = re.compile('^child\:\:')  # 匹配诸如 child::

            temp_prediction = predicate_atom
            temp_path = MongoDB_previous_path

            # todo: 要考虑 多级路径的情况 例如：child::artist/child::name='Anang Ashanty'
            match_result = pattern_axis_child_tagname_slash.match(temp_prediction)
            while (match_result != None):
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
            # todo: !=
            search_result = pattern_not_equal.search(temp_prediction)
            if (search_result != None):
                key = temp_path + "." + temp_prediction[0:search_result.span()[0]]
                value = temp_prediction[search_result.span()[1]:]
                # todo:判断是 int 还是 string
                match_result = pattern_String_value.match(value)
                if (match_result == None):
                    value = int(value)
                else:
                    value = value[1:len(value) - 1]
                if (key in filters):
                    previous_value = filters.pop(key)
                    previous_value.setdefault("$ne", value)
                    filters.setdefault(key, previous_value)
                else:
                    filters.setdefault(key, {"$ne": value})
                continue
            # todo:=
            search_result = pattern_equal.search(temp_prediction)
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

        return filters

    if (search_result_or != None):
        # todo:如果有 "or" predicate 需要拆分成小的 predicate_atom
        # 例如 child::year>=1990 or child::year<=2000
        predicate_split_result = predicate.split(sep=' or ')

        # todo：解析每个 predicate_atom 的语义
        for predicate_atom in predicate_split_result:
            # todo: 解析 axis

            # todo：axis 为 child的情况
            pattern_axis_child_tagname_slash = re.compile('^child\:\:[a-zA-Z]+\/')  # 匹配诸如 child::artist/
            pattern_axis_child = re.compile('^child\:\:')  # 匹配诸如 child::

            temp_prediction = predicate_atom
            temp_path = MongoDB_previous_path

            # todo: 要考虑 多级路径的情况 例如：child::artist/child::name='Anang Ashanty'
            match_result = pattern_axis_child_tagname_slash.match(temp_prediction)
            while (match_result != None):
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
                filters_or.append({key: {"$gte": value}})
                continue

            # todo:>
            search_result = pattern_greater_than.search(temp_prediction)
            if (search_result != None):
                key = temp_path + "." + temp_prediction[0:search_result.span()[0]]
                value = int(temp_prediction[search_result.span()[1]:])
                filters_or.append({key: {"$gt": value}})
                continue

            # todo:<=
            search_result = pattern_less_or_equal_than.search(temp_prediction)
            if (search_result != None):
                key = temp_path + "." + temp_prediction[0:search_result.span()[0]]
                value = int(temp_prediction[search_result.span()[1]:])
                filters_or.append({key: {"$lte": value}})
                continue

            search_result = pattern_less_than.search(temp_prediction)
            # todo:<
            if (search_result != None):
                key = temp_path + "." + temp_prediction[0:search_result.span()[0]]
                value = int(temp_prediction[search_result.span()[1]:])
                filters_or.append({key: {"$lt": value}})
                continue
            # todo: !=
            search_result = pattern_not_equal.search(temp_prediction)
            if (search_result != None):
                key = temp_path + "." + temp_prediction[0:search_result.span()[0]]
                value = temp_prediction[search_result.span()[1]:]
                # todo:判断是 int 还是 string
                match_result = pattern_String_value.match(value)
                if (match_result == None):
                    value = int(value)
                else:
                    value = value[1:len(value) - 1]
                filters_or.append({key: {"$ne": value}})
                continue
            # todo:=
            search_result = pattern_equal.search(temp_prediction)
            if (search_result != None):
                key = temp_path + "." + temp_prediction[0:search_result.span()[0]]
                value = temp_prediction[search_result.span()[1]:]
                # todo:判断是 int 还是 string
                match_result = pattern_String_value.match(value)
                if (match_result == None):
                    value = int(value)
                else:
                    value = value[1:len(value) - 1]
                filters_or.append({key: value})
                continue

        filters = {"$or": filters_or}
        return filters


# todo：只返回 axis::pathname[predicate] 所对应的 pathname
def generate_MongoDB_path(split_XpathQuery):
    """

    :param split_XpathQuery: parse_to_MongoDB_Query_filter 产生的split_XpathQuery；或者是 parse_to_MongoDB_Query_projection 迭代过程中产生的 split_XpathQuery 或 XpathQuery
    :return: 返回axis::pathname[predicate] 所对应的 pathname
    """
    # todo:匹配 child::
    pattern_axis_child = re.compile('^child\:\:[a-z]*')
    match_result = pattern_axis_child.match(split_XpathQuery).span()
    generate_path = split_XpathQuery[7:match_result[1]]

    # todo: 需要考虑其他axis

    return generate_path


def ApplyMongoDBQuery(database, collection, filter, projection, function):
    database = 'test'
    collection = 'library'

    # todo:连接数据库
    client = pymongo.MongoClient("localhost", 27017)
    # todo：选择数据库
    mydb = client[database]
    # todo：选择collection
    mycol = mydb[collection]
    result = mycol.find(filter, projection)
    # todo: handle functions below...
    if function == 'count':
        print("handle count() function")
    if function == 'text':
        print("handle text() function")
    else:
        print("no function detected or unknown function name")

    return result


# returns a tuple (sanitized XPath query, function name)
def data_preprocess(XpathQuery):
    # only match count() at the beginning of query
    count = re.compile('^count\((.*)\)')
    # only match text() at the end of query
    text = re.compile('/text\(\)')
    if count.match(XpathQuery):
        sanitized_query = count.split(XpathQuery)[1]
        return (sanitized_query, "count")
    if text.search(XpathQuery):
        sanitized_query = text.split(XpathQuery)[0]
        return (sanitized_query, "text")
    else:  # no function detected, return original XpathQuery and empty function name
        return (XpathQuery, "")


# ---------------descendant-------------------------------------------


# 获取find_one()的结果，previous_path为descendant节点前解析得到的路径
def mongodb_find_one(previous_path, database, collection):
    # 连接本地MongoDB,端口27017
    client = pymongo.MongoClient("localhost", 27017)
    # 选择database
    my_db = client[database]
    # 选择collection
    my_col = my_db[collection]
    # 打印其中一个text
    # previous_path作用于Xpath中descendant节点前有其他节点的情况
    if previous_path:
        find_one_result = my_col.find_one({}, {previous_path: 1})
    else:
        find_one_result = my_col.find_one()
    return find_one_result


# 找出指定database.collection中所有不同的绝对路径
def find_all_paths(dict_input, path, all_path_set):
    for key in dict_input:
        if (isinstance(dict_input[key], dict)):
            find_all_paths(dict_input[key], path + "." + str(key), all_path_set)
        elif (isinstance(dict_input[key], list)):
            for item in dict_input[key]:
                if (isinstance(item, dict)):
                    find_all_paths(item, path + "." + str(key), all_path_set)
                else:
                    all_path_set.add(path + "." + str(key))
        else:
            all_path_set.add(path + "." + str(key))


# 取出Xpath中跟descendant-or-self相对应的节点，举例：
# 输入："descendant-or-self::song/" 输出: song
def generate_descendant_or_self_path(split_xpath_query: str):
    pattern_axis_descendant = re.compile('^descendant-or-self::[a-z]*')
    match_result = pattern_axis_descendant.match(split_xpath_query).span()
    generated_descendant_short_path = split_xpath_query[20:match_result[1]]
    return generated_descendant_short_path


# 取出Xpath中跟descendant相对应的节点，举例：
# 输入："descendant::song/" 输出: song
def generate_descendant_path(split_xpath_query: str):
    pattern_axis_descendant = re.compile('^descendant::[a-z]*')
    match_result = pattern_axis_descendant.match(split_xpath_query).span()
    generated_descendant_short_path = split_xpath_query[12:match_result[1]]
    return generated_descendant_short_path


# 输入：descendant相对应的节点 输出：collection中到该节点的所有绝对路径
# 举例：输入：title 输出：{'songs.song.title', 'title'}
def parse_descendant_full_mongodb_path(generated_descendant_short_path, database, collection, previous_path):
    one_text_json = mongodb_find_one(previous_path, database, collection)
    # all_path为database.collection中所有不同的绝对路径
    all_path = set()
    find_all_paths(one_text_json, "", all_path)
    pattern_descendant_value = re.compile("(.+(?=" + generated_descendant_short_path + "))")
    matched_list = set()
    # matched_list存储到descendant相对应的节点的绝对路径
    for path in all_path:
        match_result = pattern_descendant_value.match(path)
        if match_result:
            match_result = match_result.span()
            matched_list.add(path[1:match_result[1] + len(generated_descendant_short_path)])
    return matched_list


# 输入："descendant::title/" 输出：{'songs.song.title', 'title'}
def bfs_parse_full_path_for_descendant(usage, descendant_input, database, collection, previous_path):
    if (usage == "descendant"):
        short_descendant = generate_descendant_path(descendant_input)
    else:
        short_descendant = generate_descendant_or_self_path(descendant_input)
    all_descendant_path = parse_descendant_full_mongodb_path(short_descendant, database, collection, previous_path)
    if (usage == "descendant" and previous_path in all_descendant_path):
        all_descendant_path.remove(previous_path)
    return all_descendant_path


# 输入："song.title" 输出："child::song/child::title"
def convert_pure_json_path_to_full_path(pure_json_path):
    if pure_json_path[0] != ".":
        pure_json_path = "." + pure_json_path
    full_path = pure_json_path.replace(".", "/child::")
    if full_path[0] == "/":
        full_path = full_path[1:]
    else:
        full_path = full_path[0:]
    return full_path


# descendant主函数
def convert_all_descendant_to_child(usage, xpath, database, collection, xpath_set):
    if usage == "descendant":
        pattern_axis_descendant = re.compile('descendant::[a-z]*')
    else:
        pattern_axis_descendant = re.compile('descendant-or-self::[a-z]*')
    if pattern_axis_descendant.search(xpath):
        match_result = pattern_axis_descendant.search(xpath).span()
        previous_path = ""
        if match_result[0] != 0:  # 说明descendant节点前面有路径
            previous_path_json = parse_to_MongoDB_Query_projection(xpath[:match_result[0] - 1])
            for key in previous_path_json:
                previous_path = key
            all_descendant_path = bfs_parse_full_path_for_descendant(usage,
                                                                     xpath[match_result[0]: match_result[1]], database,
                                                                     collection, previous_path)
        else:
            all_descendant_path = bfs_parse_full_path_for_descendant(usage, xpath[0: match_result[1]], database,
                                                                     collection, previous_path)
        if all_descendant_path:  # 表明截止到匹配到的descendant为止，存在从根节点开始的路径供后半部分进行匹配, 所有路径存在all_descendant_path里
            for path in all_descendant_path:
                local_path = path[len(previous_path):]
                full_local_path = convert_pure_json_path_to_full_path(local_path)
                new_xpath = xpath[:match_result[0]] + full_local_path + xpath[match_result[1]:]
                convert_all_descendant_to_child(usage, new_xpath, database, collection, xpath_set)
        else:  # 表明截止到目前为止已经不存在全路径满足条件了，直接return空list
            return
    else:
        xpath_set.add(xpath)


# ---------------descendant-------------------------------------------

# ---------------shorthand-------------------------------------------

def short_hand_convert_to_full_path(short_hand_path):
    pattern1 = re.compile('^[a-z]+/')
    pattern2 = re.compile('^/[a-z]+')
    pattern3 = re.compile('^and\s[a-z]+')
    pattern4 = re.compile('^or\s[a-z]+')
    pattern5 = re.compile('^\[[a-z]+')
    pattern6 = re.compile('//[a-z]+')
    patterns = [pattern1,pattern2,pattern3,pattern4,pattern5,pattern6]
    pointer = 0
    full_path = ""
    while pointer < len(short_hand_path):
        for i in range(len(patterns)):
            result = patterns[i].search(short_hand_path[pointer:])
            find = 0
            if result:
                find = 1
                result = result.span()
                full_path = full_path + short_hand_path[pointer:result[0]]
                print(result)
                if i == 0:
                    full_path = full_path + "child::" + short_hand_path[pointer + result[0]: pointer + result[1]-1]
                    pointer = pointer + result[1] - 1
                elif i == 1:
                    full_path = full_path + short_hand_path[pointer + result[0]] + "child::" + short_hand_path[pointer + result[0]+1: pointer + result[1]]
                    pointer = pointer + result[1]
                elif i == 2:
                    full_path = full_path + "and child::" + short_hand_path[pointer + result[0] + 4: pointer + result[1]]
                    pointer = pointer + result[1]
                elif i == 3:
                    full_path = full_path + "or child::" + short_hand_path[pointer + result[0] + 3: pointer + result[1]]
                    pointer = pointer + result[1]
                elif i == 4:
                    full_path = full_path + short_hand_path[pointer + result[0]] + "child::" + short_hand_path[pointer + result[0] + 1: pointer + result[1]]
                    pointer = pointer + result[1]
                elif i == 5:
                    full_path = full_path + short_hand_path[pointer + result[0]] + "descendant::" + short_hand_path[pointer + result[0] + 2: pointer + result[1]]
                    pointer = pointer + result[1]
                break
        if not find:
            full_path += short_hand_path[pointer]
            pointer += 1
    print(full_path)
    return full_path


# ---------------shorthand-------------------------------------------


if __name__ == '__main__':
    # test generate_MongoDB_filters function
    # temp = generate_MongoDB_filters("library", "child::album[child::year>=1990 and child::year<=2000]/")
    # temp = generate_MongoDB_filters("library", "child::album[child::artists[child::artist/child::name='Anang Ashanty']/child::artist[country='Indonesia']]/")
    # temp = generate_MongoDB_filters("library.album", "child::artists[child::artist/child::name='Anang Ashanty']/")

    # temp = generate_MongoDB_filters("library","child::album[child::artists/child::artist[child::age>=20 and child::age<=30] and child::artists/child::artist[child::country='Indonesia']]/")
    # temp = generate_MongoDB_filters("library","child::album[child::artists/child::artist/child::name='Anang Ashanty' and child::artists/child::artist[child::country='Indonesia']]/")
    # temp = generate_MongoDB_filters("library","child::album[child::artists/child::artist[child::country='Indonesia'] and child::artists/child::artist/child::name='Anang Ashanty']/")

    # test parse_predicate function
    # temp = parse_predicate('library','child::year>=1990 or child::year<=2000')
    # temp = parse_predicate('library','child::year>=1990 and child::year<=2000')


    # todo:输入Xpath查询语句 开头加上 doc('library.xml')/
    # todo: 还未通过的测试样例

    # todo: 通过的测试样例
    # 关于 or的测试样例
    # XpathQuery = "child::library/child::album[child::year>=1990 or child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name"
    XpathQuery = "child::library/child::album[child::artists/child::artist[child::age>=20 or child::age<=30] and child::artists/child::artist[child::country='Indonesia']]/child::title"


    # XpathQuery = "child::library/child::album[child::year>=1990 and child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name"
    # XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name"
    # XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name[child::age>30]"

    # XpathQuery = "child::library/child::album[child::artists/child::artist[child::name='Anang Ashanty'] and child::artists/child::artist[child::country='Indonesia']]/child::title"
    # XpathQuery = "child::library/child::album[child::artists/child::artist[child::age>=20 and child::age<=30] and child::artists/child::artist[child::country='Indonesia']]/child::title"

    # XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']/child::artist[child::country='Indonesia']]/child::title"
    # XpathQuery = "child::library/child::album[child::artists/child::artist[child::name='Anang Ashanty'] and child::artists/child::artist[child::country='Indonesia']]/child::title"
    # XpathQuery = "child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty' and child::artists/child::artist/child::country='Indonesia']/child::title"
    # XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty'] and child::artists/child::artist[child::country='Indonesia']]/child::title"
    # XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']/child::artist/child::name='Anang Ashanty' and child::artists/child::artist[child::country='Indonesia']]/child::title"
    # XpathQuery =  "count(child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name[child::age>30])"
    # XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name/text()"

    # todo:对 XpathQuery 字符串预处理
    # 去掉空格
    # strip两头

    # todo:解析 database name 和 collection name
    database = 'test'
    collection = 'library'

    # todo: XpathQuery 转 MongoDBQuery
    (sanitized_query, function) = data_preprocess(XpathQuery)
    filter = parse_to_MongoDB_Query_filter("", sanitized_query)
    projection = parse_to_MongoDB_Query_projection(sanitized_query)

    # todo MongoDBQuery
    result = ApplyMongoDBQuery(database, collection, filter, projection, function)
    
     # 测试descendant-or-self
    print("预期结果为title", bfs_parse_full_path_for_descendant("descendant-or-self", "descendant-or-self::title/", database, collection, "title"))
    print("预期结果为songs.song.title", bfs_parse_full_path_for_descendant("descendant-or-self", "descendant-or-self::title/", database, collection, "songs"))
    # 测试descendant
    print("预期结果为set()", bfs_parse_full_path_for_descendant("descendant", "descendant::title/", database, collection, "title"))
    print("预期结果为songs.song.title, title", bfs_parse_full_path_for_descendant("descendant", "descendant::title/", database, collection, ""))
    print("预期结果为songs.song.title", bfs_parse_full_path_for_descendant("descendant", "descendant::title/", database, collection, "songs"))
    
    #测试descendant主函数 输入为完整xpath, 输出将所有descendant节点置换为child节点
    Xpath_for_descendant_text = "descendant::song/descendant::title"
    xpath_set = set()
    convert_all_descendant_to_child("descendant-or-self", Xpath_for_descendant_text, database, collection, xpath_set)
    print(xpath_set)

    # test function name parsing
    print("function:", function)

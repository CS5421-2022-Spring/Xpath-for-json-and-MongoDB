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

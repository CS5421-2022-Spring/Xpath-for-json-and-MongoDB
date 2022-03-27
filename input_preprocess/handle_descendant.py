import pymongo
import re
from parse_process.parse import parse_to_MongoDB_Query_projection


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
# usage: "descendant" or "descendant-or-self"
# xpath: full_xpath_query with descendant::
# xpath_set: returned result
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
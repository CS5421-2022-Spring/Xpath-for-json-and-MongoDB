import pymongo
import re

from handle_descendant import convert_all_descendant_to_child
from handle_concise import short_hand_convert_to_full_path

# returns a list of tuples (sanitized XPath query, function name)
def data_preprocess(XpathQuery, isFullPath, database, collection):
    if (isFullPath == False):
        query = short_hand_convert_to_full_path(XpathQuery)
    else :
        query = XpathQuery
    
    xpath_set = set()
    convert_all_descendant_to_child("descendant", query, database, collection, xpath_set)
    
    xpath_set_2 = set()
    for xpath in xpath_set:
        convert_all_descendant_to_child("descendant-or-self", xpath, database, collection, xpath_set_2)
    
    result = []
    # only match count() at the beginning of query
    count = re.compile('^count\((.*)\)')
    # only match text() at the end of query
    text = re.compile('/(child::)?text\(\)')
    # match * at the end of query
    any_element = re.compile('\*$')
    for xpath in xpath_set_2:
        if count.match(xpath):
            sanitized_query = count.split(xpath)[1]
            result.append((sanitized_query, "count"))
        elif text.search(xpath):
            sanitized_query = text.split(xpath)[0]
            result.append((sanitized_query, "text"))
        elif any_element.search(xpath):
            sanitized_query = any_element.split(XpathQuery)[0][:-1]
            result.append(sanitized_query, "any_element")
        else:  # no function detected, return original XpathQuery and empty function name
            result.append((xpath, ""))
    return result


print(data_preprocess("count(child::library/descendant::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name[child::age>30])", True, "", ""))
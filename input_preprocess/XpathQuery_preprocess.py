import pymongo
import re

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
    any_element = re.complie('\*$')
    
    for xpath in xpath_set_2:   
        if count.match(xpath):
            sanitized_query = count.split(xpath)[1]
            result.append(sanitized_query, "count")
        if text.search(xpath):
            sanitized_query = text.split(xpath)[0]
            result.append(sanitized_query, "text")
        if any_element.search(xpath):
            sanitized_query = any_element.split(XpathQuery)[0][:-1]
            result.append(sanitized_query, "any_element")
        else:  # no function detected, return original XpathQuery and empty function name
            result.append(xpath, "")
    return result

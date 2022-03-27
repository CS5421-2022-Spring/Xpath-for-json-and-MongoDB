import pymongo
import re

# returns a tuple (sanitized XPath query, function name)
def data_preprocess(XpathQuery,isFullPath):

    # if (isFullPath == True):
    #
    # else:

    # only match count() at the beginning of query
    count = re.compile('^count\((.*)\)')
    # only match text() at the end of query
    text = re.compile('/(child::)?text\(\)')
    # match * at the end of query
    any_element = re.complie('\*$')
    if count.match(XpathQuery):
        sanitized_query = count.split(XpathQuery)[1]
        return (sanitized_query, "count")
    if text.search(XpathQuery):
        sanitized_query = text.split(XpathQuery)[0]
        return (sanitized_query, "text")
    if any_element.search(XpathQuery):
        sanitized_query = any_element.split(XpathQuery)[0][:-1]
        return (sanitized_query, "any_element")
    else:  # no function detected, return original XpathQuery and empty function name
        return (XpathQuery, "")


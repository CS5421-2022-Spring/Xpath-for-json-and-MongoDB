'''
Transfer output into XML string
'''
from bson.json_util import dumps
import xml.etree.ElementTree as ET
import xml.dom.minidom

def count(jsonResult):
  # import from outside
  pass

def text(jsonResult):
  # import from outside
  pass

def _parseObj(obj, key):
  # alwasy return a list of value within this key
  result = []
  nextLayer = obj[key]
  if isinstance(nextLayer,list):
    result.extend(nextLayer)
  else:
    result.append(nextLayer)
  return result
  

# parse the structure of result tree
# return a list of result value
# result only has one type of tag, not considering union
def parseNodes(jsonResult,projection):
  keys = list(projection.keys())[0]
  resultStack = jsonResult # a stack of all the un-parsed result
  resultStack_ = [] # next layer of stack of the un-parsed result
  for key in keys.split('.'):
    for obj in resultStack:
      resultStack_.extend(_parseObj(obj, key))
    resultStack = resultStack_.copy()
    resultStack_ = []
  return resultStack

def finalOutput(result,projection,function):
  # return xpath tree
  resultTree = ET.Element("result")
  if function == 'count':
    resultTree.text = count(result)
  if function == 'text':
    resultTree.text = text(result)
  else:
    resultList = parseNodes(result,projection)
    resultTag = list(projection.keys())[0].split('.')[-1]
    for res in resultList:
      resultElement = ET.SubElement(resultTree,resultTag)
      resultElement.text = res

  resultDom = xml.dom.minidom.parseString(ET.tostring(resultTree,method='xml').decode('utf-8')) # or xml.dom.minidom.parseString(xml_string)
  pretty_xml_as_string = resultDom.toprettyxml()

  return pretty_xml_as_string

if __name__=="__main__":
  import sys
  import os
  sys.path.append(os.path.join(os.getcwd(),'Xpath-for-json-and-MongoDB'))
  from parse import *

  # MongoDB database.collection
  database = 'test'
  collection = 'library'

  # Xpath query for test
  # XpathQuery = "child::library/child::album/child::artists/child::artist[child::name='Anang Ashanty']/child::name"
  # XpathQuery = "child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::songs/child::song/child::title"
  XpathQuery = "child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::genres/child::genre"
  
  # Our result
  (sanitized_query, function) = data_preprocess(XpathQuery)
  filter = parse_to_MongoDB_Query_filter("", sanitized_query)
  projection = parse_to_MongoDB_Query_projection(sanitized_query)
  cursor = ApplyMongoDBQuery(database, collection, filter, projection, function)
  result = eval(dumps(list(cursor)))
  xml_result = finalOutput(result,projection,function)
  # xml_result = dicttoxml.dicttoxml(result)  
  print("Final result:",xml_result)
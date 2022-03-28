'''
Transfer output into XML string
'''
from handle_functions import handleCount, handleText
from bson.json_util import dumps
import xml.etree.ElementTree as ET
import xml.dom.minidom
import dicttoxml

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
def _parseNodes(jsonResult,projection):
  keys = list(projection.keys())[0]
  resultStack = jsonResult # a stack of all the un-parsed result
  resultStack_ = [] # next layer of stack of the un-parsed result
  for key in keys.split('.'):
    for obj in resultStack:
      resultStack_.extend(_parseObj(obj, key))
    resultStack = resultStack_.copy()
    resultStack_ = []
  return resultStack

# return pretty output string of the XML tree
def prettyOutput(xml_string):
  resultDom = xml.dom.minidom.parseString(xml_string) # or xml.dom.minidom.parseString(xml_string)
  pretty_xml_as_string = resultDom.toprettyxml()
  return pretty_xml_as_string

# build the XML Tree from result list, return a string
def buildXMLResult(resultList,projection,hasTag=True):
    resultTree = ET.Element("result")
    for obj in resultList:
      if hasTag:
        resultTag = list(projection.keys())[0].split('.')[-1]
        if isinstance(obj,dict):
          # target node has more than one layer
          resultXML = dicttoxml.dicttoxml(obj,custom_root=resultTag,attr_type=False).decode('utf-8')
          resultXML = resultXML[39:]
          resultElement = ET.fromstring(resultXML)
        else:
          # target node only has a text layer
          resultElement = ET.Element(resultTag)
          resultElement.text = obj
        resultTree.append(resultElement)
      else:      
        # count and text only has one root element in the list
        resultElement = resultTree
        resultElement.text = obj

    return ET.tostring(resultTree).decode("utf-8")

def finalOutput(cursor,projection,operator,pretty=True):
  # fetch result
  if isinstance(cursor,list) or isinstance(cursor,int):
    result = cursor # result is already a dumped string list
  else:
    result = eval(dumps(list(cursor)))

  # parse result
  resultList = []
  tag = True
  # switch the ouput type
  if operator == 'count':
    tag = False
    resultList.append(str(handleCount(projection,result)))
  elif operator == 'text':
    tag = False
    text = handleText(projection,result)
    resultList.append(''.join(text))
  else:
    resultList = _parseNodes(result,projection)

  # build XML tree
  resultTree = buildXMLResult(resultList,projection,tag)
  if pretty:
    resultTree = prettyOutput(resultTree)

  return resultTree

if __name__=="__main__":
  import pymongo
  import sys
  import os
  sys.path.append(os.path.join(os.getcwd(),"Xpath-for-json-and-MongoDB"))

  import parse_process.parse as parse
  from input_preprocess.XpathQuery_preprocess import data_preprocess
    
  XpathQuery = "child::library/child::album/child::artists/child::artist"

  # todo:解析 database name 和 collection name
  database = 'test'
  collection = 'library'
  client = pymongo.MongoClient("localhost", 27017)
  # todo：选择数据库
  mydb = client[database]
  # todo：选择collection
  mycol = mydb[collection]

  preprocess_result = data_preprocess(XpathQuery,True,database,collection)

  # todo：parse的逻辑

  for each_preprocessed_result in preprocess_result:
      (preprocessed_xpathQuery,operator) = each_preprocessed_result
      filter = parse.parse_to_MongoDB_Query_filter("",preprocessed_xpathQuery)
      projection = parse.parse_to_MongoDB_Query_projection(preprocessed_xpathQuery)
      result = mycol.find(filter, projection)
      result = eval(dumps(list(result)))
      print("xpathQuery",XpathQuery)
      print("result",result)
      print("projection",projection)
      print("operator",operator)
      xml_result = finalOutput(result,projection,operator="",pretty=False)
    
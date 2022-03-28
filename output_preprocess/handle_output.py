'''
Transfer output into XML string
'''
from handle_functions import handleCount, handleText
from bson.json_util import dumps
import xml.etree.ElementTree as ET
import xml.dom.minidom

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
def _prettyOutput(xml_string):
  resultDom = xml.dom.minidom.parseString(xml_string) # or xml.dom.minidom.parseString(xml_string)
  pretty_xml_as_string = resultDom.toprettyxml()
  return pretty_xml_as_string

# build the XML Tree from result list, return a string
def buildXMLResult(resultList,projection,tag=True):
    resultTree = ET.Element("result")
    for res in resultList:
      if tag:
        resultTag = list(projection.keys())[0].split('.')[-1]
        resultElement = ET.SubElement(resultTree,resultTag)
      else:      
        # count and text only has one element in the list
        resultElement = resultTree
      resultElement.text = res

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
    resultList.append('\n'.join(text)+'\n')
  else:
    resultList = _parseNodes(result,projection)

  # build XML tree
  resultTree = buildXMLResult(resultList,projection,tag)
  if pretty:
    resultTree = _prettyOutput(resultTree)

  return resultTree
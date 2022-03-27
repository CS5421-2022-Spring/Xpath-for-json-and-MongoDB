
def handleCount(projection,result):
    keys = list(projection.keys())[0]
    first_key = keys.split('.')[0]
    last_key = keys.split('.')[-1]

    # count the number of nodes in the final result
    def count(last_key, dictionary):
        if isinstance(dictionary,list):
            sum = 0
            for dic in dictionary:
                sum  = sum + count(last_key,dic)
            return sum
        # Each dictionary only has one key and one value except the first dictionary.
        value = list(dictionary.values())[0]
        key = list(dictionary.keys())[0]
        # Reach to the last nodes.
        if key == last_key:
            if isinstance(value, list):
                return len(value)
            else:
                return 1
        # The corresponding value is a dictionary, search this dictionary
        if isinstance(value, dict):
            return count(last_key, value)
        # The corresponding value is a list, for each dictionary in the list, search the dictionary.
        # And sum up the result.
        if isinstance(value, list):
            sum = 0
            for dic in value:
                sum  = sum + count(last_key,dic)
            return sum

    count_result = 0
    for doc in result:
        #temp = doc[keys.split('.')[0]]
        temp = doc[first_key]
        number = count(last_key,temp)
        count_result = count_result + number

    return count_result

def handleText(projection,result):
    keys = list(projection.keys())[0]
    first_key = keys.split('.')[0]
    last_key = keys.split('.')[-1]

    # Directly print the text node if the query result is text node. If not, return NONE.
    def text(last_key, dictionary):
        if isinstance(dictionary,list):
            lst = []
            for dic in dictionary:
                lst.append(text(last_key, dic))
            return lst
        value = list(dictionary.values())[0]
        key = list(dictionary.keys())[0]
        # print(value)
        # print(key)
        if key == last_key:
            if isinstance(value, list) and isinstance(value[0], str):
                return value
            elif isinstance(value, dict) or isinstance(value, list):
                return
            # elif isinstance(value, list) and isinstance(value[0], str):
            #     return value
            else:
                return value
        if isinstance(value, dict):
            return text(last_key, value)
        if isinstance(value, list):
            lst = []
            for dic in value:
                lst.append(text(last_key,dic))
            return lst

    text_result = []
    # print(type(text_result))
    for doc in result:
        #temp = doc[keys.split('.')[0]]
        temp = doc[first_key]
        temp_result = text(last_key,temp)
        text_result.append(temp_result)

    def flatten(list_of_lists):
        if len(list_of_lists) == 0:
            return list_of_lists
        if isinstance(list_of_lists[0], list):
            return flatten(list_of_lists[0]) + flatten(list_of_lists[1:])
        return list_of_lists[:1] + flatten(list_of_lists[1:])

    return flatten(text_result)

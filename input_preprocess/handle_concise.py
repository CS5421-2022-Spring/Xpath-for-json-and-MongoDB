import re
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
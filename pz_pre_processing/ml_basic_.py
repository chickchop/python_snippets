def activity_to_symbol(acti_seriese):
    acti_key = list(set(acti_seriese))
    end = 33 + len(acti_key)
    acti_value = [chr(i) for i in range(33,end)]
    acti_dic = { k:v for (k,v) in zip(acti_key, acti_value)}

    symbol_list = []
    for i in acti_seriese:
        j = acti_dic[i]
        symbol_list.append(j)
    
    return symbol_list , acti_dic

def symbol_to_activity(symbol_list, acti_dic):
    symbol_dic = {v:k for k,v in acti_dic.items()}

    acti_list = []
    for i in symbol_list:
        j = symbol_dic[i]
        acti_list.append(j)

    return acti_list

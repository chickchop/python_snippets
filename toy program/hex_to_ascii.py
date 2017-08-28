'''
Created on 2017. 7. 24.

@author: ko
'''
import binascii


def hex_to_ascii(num):
    # decimal - > hex
    hex_string = hex(num);
    
    #print("hex value")
    #print(hex_string)
    
    #hex sting -> binary string
    hex_string = hex_string[2:]
    hex_binary = binascii.unhexlify(hex_string)
    #print("hex binary")
    #print(hex_binary)
    
    
    #hex binary- > ascii
    work_order_value = hex_binary.decode('ascii', 'replace')
    
    #print('work order')
    print(work_order_value)
    
    return work_order_value
    


def make_work_order(ps_data):
    work_orders = []
    
    for i in ps_data:
        work_order_ = hex_to_ascii(i)
        work_orders.append(work_order_)
        
    
    work_order = ''.join(work_orders)
    print(work_order) 
  
num = 14402
hex_to_ascii(num)
    
#ps_data = [22351, 12593, 14128, 14129, 12848, 12336, 13623, 14080]
#make_work_order(ps_data)
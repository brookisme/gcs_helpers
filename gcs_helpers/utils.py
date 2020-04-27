import json


def read_json(path,*key_path):
    with open(path,'r') as file:
        jsn=json.load(file)
    for k in key_path:
        jsn=jsn[k]
    return jsn


def write_json(obj,path):
    with open(path,'w') as file:
        jsn=json.dump(obj,file)
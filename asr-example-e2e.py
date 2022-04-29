import collections
import numpy as np
import os
import time
import base64
import json
from scipy.io import wavfile
import requests

from concurrent.futures import ThreadPoolExecutor
import threading

#ASR_URL = 'http://record-asr.lizhifm.com'
#ASR_URL = 'http://0.0.0.0:10086'
#ASR_URL = 'http://127.0.0.1:10085'
ASR_URL = 'http://localhost:10086'
#ASR_URL = 'http://127.0.0.1:10087'

def read_list(path):
    with open(path, 'r') as f:
        lines = f.readlines()
        f.close()
        rt = [item.strip() for item in lines]
        return rt

def post_one(path):
    name = os.path.basename(path)
    with open(path, 'rb') as f:
        content = f.read()
        f.close()
        encoded_str = base64.b64encode(content)
        basee64_file = encoded_str.decode('utf-8')
        body = {'audioBase64': basee64_file, 'lang': 1, 'scene': 0, 'aue': 'm4a', 'id': name}

        # 发起请求
        st = time.time()
        try:
            r=httpx.post(ASR_URL, json=body, timeout=20)
            if r.status_code != 200:
                print('{}\trequests fails code:{} details:{}'.format(name, r.status_code, r.json()))
            else:
                #print('result: {}'.format(r.json()['result']))
                end = time.time()
                consume = end - st
                text = r.json()['result']
                print('{}\t{}\t{:.3f}'.format(name, text, consume))
        except Exception as e:
                print(e)
        except httpx.ReadTimeout:
            print('{}\tError {}'.format(name, 'httpx.readtimeout'))

def post_list(paths):
    for path in paths:
        post_one(path)

def split_list(paths, num):
    n = len(paths)
    each = int((n+1)/num)
    rt = []
    for i in range(num-1):
        rt.append(paths[each*i: each*(i+1)])
    rt.append(paths[(num-1)*each:])
    return rt

if __name__ == '__main__':
    path = '/DATA/disk1/duyao/workplace/e2e-release/record1.wav'
    name = os.path.basename(path)
    with open(path, 'rb') as f:
        content = f.read()
        f.close()
        encoded_str = base64.b64encode(content)
        basee64_file = encoded_str.decode('utf-8')

    #aue set to wav or mp3 or m4a
    #body = {'audioBase64': basee64_file, 'lang': 1, 'scene': 0, 'aue': 'wav', 'id': name}
    body = {'audioBase64': basee64_file, 'lang': 1, 'scene': 0, 'aue': 'wav', 'id': name}

    # 发起请求
    st = time.time()
    r = requests.post(ASR_URL, json=body, timeout=300)
    if r.status_code != 200:
        print('requests fails code:{}'.format(r.status_code))
    else:
        end = time.time()
        consume = end - st
        #print(r.json())
        text = r.json()['result']
        timestamps = r.json()['timestamps']
        #for i in range(len(sentences)):
        #  print(type(sentences[i]), sentences[i])
        print('{}\t{}\t{:.3f}'.format(name, text, consume))

import collections
import numpy as np
import os
import time
import base64
import json
from scipy.io import wavfile
import requests
import hmac
import hashlib
from hashlib import sha256

from concurrent.futures import ThreadPoolExecutor
import threading

#ASR_URL = 'http://record-asr.lizhifm.com'
#ASR_URL = 'http://0.0.0.0:10086'
#ASR_URL = 'http://127.0.0.1:10085'
#ASR_URL = 'http://localhost:10086'
#ASR_URL = 'http://127.0.0.1:10087'
ASR_URL = 'http://127.0.0.1:10086'
#ASR_URL = 'http://47.95.223.174:8088'
#ASR_URL = 'http://47.95.223.174:80'


userId = "shangjiao"
key = "shangjiaokey"

timestamp = str(int(time.time()))
signature = hmac.new(key.encode('utf-8'), (userId + timestamp).encode('utf-8'), digestmod=sha256).hexdigest()
headers = {"x-dev-id": str(userId),
          'x-request-send-timestamp': timestamp, 
          "x-signature": signature}



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

def write_timestamps(ts, name, consume):
  name='ts-{}.txt'.format(name)
  sts=[]
  ends=[]

  for i,item in enumerate(ts):
    sts.append(item['start'])
    ends.append(item['end'])
  n = len(ends)
  pauses = []
  for i in range(n-1):
    pauses.append(sts[i+1]-ends[i])
  pauses.append(300)

  with open(name, 'w') as f:
    for i,item in enumerate(ts):
      f.write('{:03d}\t{:0.3f}\t{:0.2f}\t{:0.2f}\t{}\n'.format(i, pauses[i], item['start'], item['end'], item['sentence']))
    f.write('total time consume {}'.format(consume))
    f.close()

def test_one(path):
    name = os.path.basename(path)
    with open(path, 'rb') as f:
        content = f.read()
        f.close()
        #enc_st = time.time()
        encoded_str = base64.b64encode(content)
        basee64_file = encoded_str.decode('utf-8')
        #enc_end = time.time()
        #consume = enc_end - enc_st
        #print('enc consume : {}'.format(consume))

    #aue set to wav or mp3 or m4a
    #body = {'audioBase64': basee64_file, 'lang': 1, 'scene': 0, 'aue': 'wav', 'id': name}
    #body = {'audioBase64': basee64_file, 'lang': 1, 'scene': 0, 'aue': 'wav', 'id': name, 'custom_words':'李长亮'}
    #body = {'audioBase64': basee64_file, 'lang': 1, 'scene': 0, 'aue': 'wav', 'id': name, 'custom_words':'蔚来汽车'}
    #body = {'audioBase64': basee64_file, 'lang': 1, 'scene': 0, 'aue': 'wav', 'id': name}
    body = {'audioBase64': basee64_file, 'lang': 1, 'scene': 0, 'aue': 'mp3', 'id': name, 'custom_words':'丽泽商城|欧伯|孔令款|三弦|孔楼村|捕蛇者说|张育良|枉凝眉|郭嘉|临江'}

    # 发起请求
    st = time.time()
    try:
        r = requests.post(ASR_URL, json=body, headers=headers, timeout=300)
    except requests.exceptions.ReadTimeout:
        print("{} request timeout".format(path))
        pass

    if r.status_code != 200:
        print('requests fails code:{}'.format(r.status_code))
    else:
        end = time.time()
        consume = end - st
        #print(r.json())
        text = r.json()['result']
        ts = r.json()['timestamps']
        #for i in range(len(sentences)):
        #  print(type(sentences[i]), sentences[i])
        #print('{}\t{}\t{:.3f}'.format(name, text, consume))
        write_timestamps(ts, name, consume)

if __name__ == '__main__':
    #path = '/DATA/disk1/duyao/workplace/e2e-release/record1.wav'
    path = '/DATA/disk1/duyao/workplace/wenet/runtime/server/x86/weilaiqiche1.wav'
    path = '/DATA/disk1/duyao/workplace/data-repo/open-audio/aishell4/test/mp3/L_R003S01C02.mp3'
    path = '/mnt/8t/openslr/aishell4/test/mp3/L_R003S01C02.mp3'
    print(time.ctime())
    paths=read_list('flist')
    paths = paths*72
    #paths=['ludingji01-60min.mp3']
    #paths=['customwords.mp3']
    #paths=['L_R003S02C02-257-267.mp3']
    for path in paths:
        print(path)
        test_one(path)

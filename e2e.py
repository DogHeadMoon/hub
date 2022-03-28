from flask import Flask, request
import requests
from pydub import AudioSegment
import contextlib
import wave
import numpy as np
from scipy.io import wavfile
import sys
import collections
import numpy as np
import os
import time

from concurrent.futures import ThreadPoolExecutor
import threading
import base64
import json
import datetime

import asyncio
import httpx
from vad import *
import io

from offline_client import SpeechClient
import tritonclient.grpc as grpcclient
from collections import defaultdict

import logging
from logging import FileHandler

app = Flask(__name__)
#url= 'http://127.0.0.1:10085/lizhi-asr'
url='localhost:8001'
id=1



def concat_parts(paths, dic):
  rt = ''
  for path in paths:
    if path in dic.keys():
      part_rt = dic[path] 
      if len(part_rt) > 2 :
        print(len(part_rt), part_rt)
        rt +=  part_rt + '，'
    else:
      app.logger.error('error : {} not in result keys'.format(path))
  if len(rt) > 0:
    if rt[-1] == '，':
      rt=rt[:-1]
  return rt

def single_job(client_files):
    rt = {}
    with grpcclient.InferenceServerClient(url=url,
                                          verbose=False) as triton_client:
        protocol_client = grpcclient
        speech_client = SpeechClient(triton_client, 'attention_rescoring', protocol_client)
        idx, audio_files = client_files
        #predictions = []
        for li in audio_files:
            result = speech_client.recognize(li, idx) 
            #predictions += result
            rt[li] = result[0]
    return rt 

def export_parts(name, pcm_parts):
  dir='/DATA/disk1/duyao/workplace/asr/frontend/py-service/parts'
  new_paths= []
  for i, pcm in enumerate(pcm_parts):
    new_path='{}/{}-{}.wav'.format(dir, name, i)
    s = io.BytesIO(pcm)
    sample_width=2
    frame_rate=16000
    channels=1
    audio = AudioSegment.from_raw(s, sample_width=2, frame_rate=16000, channels=1).export(new_path, format='wav')
    s.close()
    #sound = AudioSegment.from_file(pcm, format='wav').set_frame_rate(16000).set_channels(1)
    new_paths.append(new_path)
  return new_paths
  
#@app.route('/lizhi-asr', methods=['GET', 'POST'])
@app.route('/', methods=['GET', 'POST'])
def process():
    rt = ''
    app.logger.error('request method : {}'.format(request.method))
    if request.method == 'POST':
      if request.is_json:
        json_data = request.json
        base64_msg = json_data['audioBase64']
        audio_bytes = base64.b64decode(base64_msg)
        aue = json_data['aue']
        name = json_data['id']
        n = len(audio_bytes)

        content = None
        if aue == 'm4a':
            buff = io.BytesIO(audio_bytes)
            sound = AudioSegment.from_file(buff, format='m4a').set_frame_rate(16000).set_channels(1)
            buff.close()
            content = sound.raw_data
        elif aue == 'wav':
            buff = io.BytesIO(audio_bytes)
            sound = AudioSegment.from_file(buff, format='wav').set_frame_rate(16000).set_channels(1)
            buff.close()
            content = sound.raw_data
        elif aue == 'mp3':
            buff = io.BytesIO(audio_bytes)  
            sound = AudioSegment.from_file(buff, format='mp3').set_frame_rate(16000).set_channels(1)
            buff.close()
            content = sound.raw_data
        elif aue == 'pcm':
            ts=datetime.datetime.now().strftime("%m-%d_%H-%M-%S_%f")
            new_path='inhouse-parts/{}_{}.pcm'.format(ts, name)
            print(new_path)
            buff = io.BytesIO(audio_bytes)  
            AudioSegment.from_raw(buff, sample_width=2, frame_rate=16000, channels=1).export(new_path, format='wav')
            buff.close()
            dic_rt = single_job((1, [new_path]))
            rt = ''
            if new_path in dic_rt:
              rt = dic_rt[new_path]
            js = {'result':rt}
            js_rt=json.dumps(js)
            return js_rt
            #sound = AudioSegment.from_raw(io.BytesIO(audio_bytes), sample_width=2, channels=1, frame_rate=16000)
            #content = audio_bytes
        elif aue == 'whole_pcm':
            content = audio_bytes
        else:
            return
        avad=Vad()
        vad_st = time.time()
        parts = avad.get_parts(content)
        vad_end = time.time()
        #contents.append(content)
        print('before export parts, name {}'.format(name))
        paths=export_parts(name, parts)
        print('after export parts')
        for i in range(len(parts)):
          print('part {} len : {}'.format(i, len(parts[i])))
    
        global id
        if len(paths) > 0: 
          dic_rt = single_job((id, paths))
          rt = concat_parts(paths, dic_rt)
          id+=1
          app.logger.info("{}\t{}".format(name, rt))
    js = {'result':rt}
    js_rt=json.dumps(js)
    return js_rt

if __name__ == '__main__':
    app.debug = True
    handler = logging.FileHandler('flask.log')
    app.logger.addHandler(handler)

    app.run(host='0.0.0.0', port=10086, threaded=True, debug=True)

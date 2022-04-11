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

import websockets
import logging
from logging import FileHandler

app = Flask(__name__)
#url= 'http://127.0.0.1:10085/lizhi-asr'
url='localhost:8001'
id=1

def concat_parts(paths, dic, boundaries):
  val_texts = []
  val_boundaries = []
  rt = ''
  for i,path in enumerate(paths):
    if path in dic.keys():
      part_rt = dic[path] 
      if len(part_rt) > 1 :
        val_texts.append(part_rt)
        val_boundaries.append(boundaries[i])
    else:
      app.logger.error('error : {} not in result keys'.format(path))
  return val_texts, val_boundaries

def single_job(client_files):
    rt = {}
    with grpcclient.InferenceServerClient(url=url,
                                          verbose=False) as client:
        protocol_client = grpcclient
        speech_client = SpeechClient(client, 'attention_rescoring', protocol_client)
        idx, audio_files = client_files
        for li in audio_files:
            result = speech_client.recognize(li, idx) 
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
  

def get_st_msg(custom_words):
  message = {}
  message['signal'] = 'start'
  message['nbest'] = 1
  message['continuous_decoding'] = False
  if len(custom_words) > 0:
    message['custom_words'] = custom_words
  msg = json.dumps(message)
  return msg

def get_end_msg():
  end_msg={'signal':'end'}
  msg = json.dumps(end_msg)
  return msg

async def post_cpu(pcm, name, custom_words):
  try:
    async with websockets.connect('ws://localhost:10089') as websocket:
        await websocket.send(get_st_msg(custom_words))
        rsp_js = await websocket.recv()
        rsp = json.loads(rsp_js)
        if 'status' in rsp.keys():
          if rsp['status'] == 'ok':
            await websocket.send(pcm)
            await websocket.send(get_end_msg())
            rt = await websocket.recv()
            print("name : {}, rt : {}".format(name, rt))
            final = json.loads(rt)
            print("after final")
            text = final['nbest']
            if websocket.open:
              await websocket.close()
            return (name, text)
  except Exception as e:
    return (name, text)

def post_multi_cpu(name, pcms, custom_words, boundaries):
  n = len(pcms)
  #loop = asyncio.get_event_loop()
  loop1 = asyncio.new_event_loop()
  asyncio.set_event_loop(loop1)
  loop = asyncio.get_event_loop()
  batch=100
  nfulls = n//batch
  nresidue = n%batch
  names = []
  val_rt = []
  rt_dic = {}
  val_boundaries = []

  if nfulls:
    for i in range(nfulls):
      tasks = []
      for j in batch:
        new_name = "{}-{}".foramt(name, i*batch + j)
        names.append(new_name)
        tasks.append(post_cpu(pcms[i*batch + j], new_name))
      done, pending = loop.run_until_complete(asyncio.wait(tasks, timeout=100))
      print("done", done)
      for fut in done:
        items = fut.result()
        print(items)
        if len(items) == 2:
          key = items[0]
          text = items[1]
          rt_dic[key] = text
  if nresidue:
    tasks = []
    for j in range(nresidue):
      new_name = "{}-{}".format(name, nfulls*batch + j)
      names.append(new_name)
      tasks.append(post_cpu(pcms[nfulls*batch + j], new_name, custom_words))
    done, pending = loop.run_until_complete(asyncio.wait(tasks, timeout=100))
    print("done", done)
    for fut in done:
      items = fut.result()
      print(items)
      if len(items) == 2:
        key = items[0]
        text = items[1]
        rt_dic[key] = text
  
  for i,key in enumerate(names):
    if key in rt_dic.keys():
      val_boundaries.append(boundaries[i])
      val_rt.append(rt_dic[key])
  return val_rt, val_boundaries

def get_timestamps_json(texts, boundaries):
  n=min(len(texts), len(boundaries))
  segs = []
  for i in range(n):
    seg={}
    seg['sentence']=texts[i]
    seg['start']=boundaries[i][0]
    seg['end']=boundaries[i][1]
    segs.append(seg)
  return segs
  

@app.route('/', methods=['GET', 'POST'])
def process():
    rt = ''
    timestamps = []
    #app.logger.info('request method : {}'.format(request.method))
    if request.method == 'POST':
      if request.is_json:
        json_data = request.json
        base64_msg = json_data['audioBase64']
        audio_bytes = base64.b64decode(base64_msg)
        aue = json_data['aue']
        name = json_data['id']
        n = len(audio_bytes)
        custom_words =''
        if 'custom_words' in json_data.keys():
          custom_words = json_data['custom_words']
          print('custom_words : ', custom_words)

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
        parts, boundaries = avad.get_parts(content)

        if len(parts):
          if len(custom_words):
            texts, boundaries = post_multi_cpu(name, parts, custom_words, boundaries)
            timestamps = get_timestamps_json(texts, boundaries)
          else:
            paths=export_parts(name, parts)
            global id
            if len(paths): 
              dic_rt = single_job((id, paths))
              texts, boundaries = concat_parts(paths, dic_rt, boundaries)
              timestamps = get_timestamps_json(texts, boundaries)
              id+=1
        if len(texts):
          rt = '，'.join(texts)
          rt += '。'

        #print('after export parts')
        #for i in range(len(parts)):
        #  print('part {} len : {}'.format(i, len(parts[i])))
        app.logger.info("{}\t{}".format(name, rt))
    js = {'result':rt, 'timestamps':timestamps}
    js_rt=json.dumps(js)
    print(js_rt)
    return js_rt

if __name__ == '__main__':
    app.debug = True
    handler = logging.FileHandler('flask.log')
    app.logger.addHandler(handler)

    app.run(host='0.0.0.0', port=10086, threaded=True, debug=True)

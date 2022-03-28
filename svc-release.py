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

contents=[]

app = Flask(__name__)

url= 'http://127.0.0.1:10085/lizhi-asr'

def post_wav(url, pcm, name):
  encoded_string = base64.b64encode(pcm)
  basee64_file = encoded_string.decode('utf-8')
  body = {'audioBase64': basee64_file, 'lang': 1, 'scene': 0, 'aue': 'pcm', 'id': name}
  r = httpx.post(url, json=body, timeout=30)
  rt = ''
  if r.status_code != 200:
    print('statu is not 200')
  else:
    rt = r.json()['result']
  return rt

async def post_one(client, url, pcm, name):
    encoded_string = base64.b64encode(pcm)
    basee64_file = encoded_string.decode('utf-8')
    body = {'audioBase64': basee64_file, 'lang': 1, 'scene': 0, 'aue': 'pcm', 'id': name}
    rt = ''
    try:
        r = await client.post(url, json=body, timeout=30)
        if r.status_code != 200:
            print('statu is not 200')
        else:
            rt = r.json()['result']
    except httpx.ConnectError as e:
        print(e)
    return rt

async def post_multi(parts, filename):
    async with httpx.AsyncClient() as client:
        tasks = []
        n = len(parts)
        for i in range(n):
            name = '{}-{}'.format(filename, i)
            tasks.append(asyncio.ensure_future(post_one(client, url, parts[i], name)))
        parts_rt = await asyncio.gather(*tasks)

        raw_t = ''
        for i in range(len(parts_rt)):
            raw_t += parts_rt[i] + ' '

        filt = []
        refine = True
        if refine:
            for item in parts_rt:
                if item :
                   filt.append(item)
        if len(filt)>1:
            rt = '，'.join(filt)
        elif len(filt)==1:
            rt = filt[0]
        else:
            rt = ''
        return  rt

#@app.route('/lizhi-asr', methods=['GET', 'POST'])
@app.route('/', methods=['GET', 'POST'])
def process():
    rt = ''
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
            sound = AudioSegment.from_file(io.BytesIO(audio_bytes), format='m4a').set_frame_rate(16000).set_channels(1)
        elif aue == 'wav' or aue == 'pcm':
            sound = AudioSegment.from_file(io.BytesIO(audio_bytes), format='wav').set_frame_rate(16000).set_channels(1)
        elif aue == 'mp3':
            sound = AudioSegment.from_file(io.BytesIO(audio_bytes), format='mp3').set_frame_rate(16000).set_channels(1)
        else:
            return
        content = sound.raw_data

        avad=Vad()
        vad_st = time.time()
        parts = avad.get_parts(content)
        vad_end = time.time()
        contents.append(content)

        rt = asyncio.run(post_multi(parts, name))
      else:
        content = request.files.get('file').read()
        rt = post_wav(url, content, 'demo.wav')
    else:
        return

    js = {'result':rt}
    js_rt=json.dumps(js)
    return js_rt

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10086, threaded=True, debug=True)

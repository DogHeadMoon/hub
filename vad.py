import webrtcvad

class Frame(object):
    def __init__(self, bytes, timestamp, duration):
        self.bytes = bytes
        self.timestamp = timestamp
        self.duration = duration

def frame_generator(frame_duration_ms, audio, sample_rate):
    n = int(sample_rate * (frame_duration_ms / 1000.0) * 2)
    offset = 0
    timestamp = 0.0
    duration = (float(n) / sample_rate) / 2.0
    while offset + n < len(audio):
        yield Frame(audio[offset:offset + n], timestamp, duration)
        timestamp += duration
        offset += n

def vad_collector(sample_rate, frame_duration_ms,
                  padding_duration_ms, vad, frames):
    num_padding_frames = int(padding_duration_ms / frame_duration_ms)
    ring_buffer = collections.deque(maxlen=num_padding_frames)
    triggered = False
    voiced_frames = []
    is_speech_list = []
    is_speech_list2 = []

    for frame_idx, frame in enumerate(frames):
        is_speech = vad.is_speech(frame.bytes, sample_rate)
        is_speech_list.append(1 if is_speech else 0)
        if not triggered:
            ring_buffer.append((frame, is_speech, frame_idx))
            num_voiced = len([f for f, speech, f_idx in ring_buffer if speech])
            if num_voiced > 0.9 * ring_buffer.maxlen:
                triggered = True
                for f, s, i in ring_buffer:
                    voiced_frames.append(i)
                ring_buffer.clear()
        else:
            voiced_frames.append(frame_idx)
            ring_buffer.append((frame, is_speech))

class Vad:
    def __init__(self):
        self.vad = webrtcvad.Vad()
        self.vad.set_mode(1)
        self.sample_rate = 16000
        self.min_speak_ms = 500;
        self.min_pause_ms = 30;
        self.acts=[]

    def frame_generator(self, frame_duration_ms, audio):
        n = int(self.sample_rate * (frame_duration_ms / 1000.0) * 2)
        offset = 0
        timestamp = 0.0
        while offset + n < len(audio):
            yield audio[offset:offset + n]
            offset += n

    def get_acts(self, pcm_data):
        is_speech_list = []
        frames = self.frame_generator(10, pcm_data)
        for frame_idx, frame in enumerate(frames):
            is_speech_list.append(self.vad.is_speech(frame, self.sample_rate))
        return is_speech_list

    def get_act_percent(self, st, end):
        n = end - st + 1
        nact = 0
        for i in range(st, end + 1):
            if self.acts[i]==1:
                nact += 1
        return nact/n

    def merge_seg(self, sts, ends):
        num = min(len(sts), len(ends))
        merged_filt_sts=[]
        merged_filt_ends=[]

        merged = []
        min_speak_ms=1500
        for i in range(num):
            if (ends[i] - sts[i])*10 < min_speak_ms:
                merged.append(True)
            else:
                merged.append(False)

        i=0
        while i<num-1:
            merged_filt_sts.append(sts[i])
            while i<num-1:
                if merged[i]==True:
                    i+=1
                else:
                    break
            merged_filt_ends.append(ends[i])
            i+=1
        if len(merged)>=2:
            if merged[-2]==False:
                merged_filt_sts.append(sts[num-1])
                merged_filt_ends.append(ends[num-1])
        return merged_filt_sts, merged_filt_ends

    def cut(self, sts, ends):
        n = len(ends)
        limit = 1000
        new_sts = []
        new_ends = []
        for i in range(n):
            dur = ends[i] - sts[i]
            if dur <= limit:
                #print(dur, self.get_act_percent(sts[i], ends[i]) )
                #if dur>=150 and self.get_act_percent(sts[i], ends[i]) > 0.6:
                if dur>=100 and self.get_act_percent(sts[i], ends[i]) > 0.6:
                    new_sts.append(sts[i])
                    new_ends.append(ends[i])
            else:
                num = int(dur/limit)
                residue = dur%limit
                for j in range(num):
                    new_sts.append(sts[i] + j*limit)
                    new_ends.append(sts[i] + (j+1)*limit)
                if(residue > 200):
                    new_sts.append(sts[i] + num*limit)
                    new_ends.append(ends[i])
        return new_sts, new_ends

    def get_window_smooth(self, i):
        window_size=5
        st = i - window_size
        if st<0:
            st=0
        n=i-st+1
        acc = 0
        for t in range(st, i+1):
            acc += self.acts[t]
        return acc/n

    def get_parts(self, pcm_data):
        parts = []
        self.acts=self.get_acts(pcm_data)
        prev = 0
        start = 0
        end = 0
        sts = []
        ends = []
        speech_acc = 0
        sil_acc = 0
        min_acc_speech = 50
        min_pause_frames = 3
        min_speak_frames = 50
        

        num = len(self.acts)
        for i in range(num):
            #if self.acts[i] == 1:
            if self.get_window_smooth(i) >= 0.6:
                speech_acc += 1
                sil_acc = 0
                if speech_acc == min_acc_speech and len(sts) == len(ends):
                    sts.append(i - speech_acc + 1)
            else:
                sil_acc += 1
                speech_acc = 0
                if sil_acc == min_pause_frames  and len(sts) == len(ends) +1:
                    ends.append(i)
            
            if i==num-1:
                if len(sts) == len(ends) +1:
                    st = sts[-1] 
                    dur = num - st
                    if self.get_act_percent(st, num-1)>0.6 and dur>=min_speak_frames:
                        ends.append(num-1)
                    else:
                        sts.pop()
                    
            #if prev==0 and self.acts[i]==1 and i!=(num-1) :
            #    sts.append(i)
            #elif prev==1 and (self.acts[i]==0 or i==(num-1)) :
            #    ends.append(i)
            #prev = self.acts[i]
        
        parts=[]
        boundaries=[]
        num = min(len(sts), len(ends))
        if num==0:
            return parts, boundaries
        elif num==1:
            dur=ends[0]-sts[0]
            if dur*10 < self.min_speak_ms:
                return parts,boundaries
            else:
                sts, ends = self.cut(sts, ends)
                npart = len(sts)
                for i in range(npart):
                    bt_st = int(sts[i]*10*0.001*self.sample_rate*2)
                    bt_end = int(ends[i]*10*0.001*self.sample_rate*2)
                    parts.append(pcm_data[bt_st:bt_end])
                    boundaries.append((sts[i]*0.01, ends[i]*0.01))
                return parts, boundaries

        #merge = False
        #if merge:

        print(sts)
        print(ends)
        n = min(len(sts), len(ends))
        pauses=[]
        for i in range(n-1):
            pauses.append(sts[i+1]-ends[i])
        print(pauses)

        merged_filt_sts = []
        merged_filt_ends = []

        merged=[]
        for i in range(num-1):
            gap = sts[i+1] - ends[i]
            if gap * 10 < self.min_pause_ms:
                merged.append(True)
            else:
                merged.append(False)

        i=0
        while i<num-1:
            merged_filt_sts.append(sts[i])
            while i<num-1:
                if merged[i]==True:
                    i+=1
                else:
                    break
            merged_filt_ends.append(ends[i])
            i+=1
        if merged[-1]==False:
            merged_filt_sts.append(sts[num-1])
            merged_filt_ends.append(ends[num-1])

        i=0
        filt_sts = []
        filt_ends = []

        num = min(len(merged_filt_sts), len(merged_filt_ends))
        print("before merged parts num : {}".format(num))
        if num>=2:
            merged_filt_sts, merged_filt_ends = self.merge_seg(merged_filt_sts, merged_filt_ends)
        num = min(len(merged_filt_sts), len(merged_filt_ends))
        print("merged parts num : {}".format(num))
        for i in range(num):
                if (merged_filt_ends[i]-merged_filt_sts[i])*10<self.min_speak_ms:
                    continue
                else:
                    filt_sts.append(merged_filt_sts[i])
                    filt_ends.append(merged_filt_ends[i])

        filt_sts, filt_ends = self.cut(filt_sts, filt_ends)

        min_size = min(len(filt_sts), len(filt_ends))
        for i in range(min_size):
            st=int(filt_sts[i]*10*0.001*self.sample_rate*2)
            end=int(filt_ends[i]*10*0.001*self.sample_rate*2)
            parts.append(pcm_data[st:end])
            boundaries.append((filt_sts[i]*0.01, filt_ends[i]*0.01))
        return parts, boundaries

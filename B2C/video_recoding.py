# -*- coding: utf-8 -*-
import subprocess

import time
import datetime
import os
import threading
import pandas as pd

class video_recoding(threading.Thread):
    
    def __init__(self, shop_code, acc, pw, ip, port, ch, open_t, close_t, ch_name):
        threading.Thread.__init__(self)
        self.shop_code = shop_code
        self.acc = acc
        self.pw = pw
        self.ip = ip
        self.port = str(port)
        self.ch = str(ch)
        self.open_t = open_t
        self.close_t = close_t
        self.ch_name = ch_name
    
    def get_rtsp_addr(self):
        #rtsp://admin:qtumai123456@192.168.1.111:554/cam/realmonitor?channel=1
        #add = "rtsp://" + self.acc + ":" + self.pw + "@" + self.ip + ":" + self.port + "/\"" + self.ch + "\""
        add = '"rtsp://' + self.acc + ':' + self.pw + '@' + self.ip + ':' + self.port + '/' + self.ch + '"'
        #add = 'rtsp://' + self.acc + ':' + self.pw + '@' + self.ip + '/' + self.ch
        return add
    
    def get_save_path(self):
        save_path = '/home/pi/workspace/stream/save_video'
        #save_path = '/home/qtumai/workspace/stream/save_video/'
        if not os.path.exists(save_path):
            os.mkdir(save_path)
        
        return save_path
    
    def get_file_name(self):
        file_name = ''
        ntime = datetime.datetime.now()
        file_name = ntime.strftime('%Y%m%d%H%M%S')
        #file_name = file_name + '_' + self.shop_code + '_' + self.ch + '.avi'
        file_name = file_name + '_' + self.shop_code  + '_' + self.ch_name + '.avi'
        return file_name
    
    def working_hours(self):
        now_t = datetime.datetime.now()
        open_t = datetime.datetime(now_t.year, now_t.month, now_t.day, self.open_t[0], self.open_t[1], 0)
        close_t = datetime.datetime(now_t.year, now_t.month, now_t.day, self.close_t[0], self.close_t[1], 0)
        self.shutdown = close_t.strftime('%H%M')
        if open_t < now_t:
            if close_t > now_t:
                return True
            else:
                return False
    
    def recode(self):
        if self.working_hours() == True:
            add = self.get_rtsp_addr()
            save_path = self.get_save_path()
            file_name = self.get_file_name()
            output = os.path.join(save_path, file_name)
            
           
            save_path = self.get_save_path()
            
            #ffmpeg -y -hide_banner -rtsp_transport tcp -i rtsp://admin:qtumai123456@192.168.1.110:554/cam/realmonitor?channel=1&subtype=0 -t 1800 -an /home/pi/workspace/stream/save_video/20210313123108032940_JJIN-ch1_cam/realmonitor?channel=1&subtype=0.avi
            cmd = ('ffmpeg -hide_banner -stimeout 10000000 -y -rtsp_transport tcp -i {} -vcodec copy -r 30 -t 1800 {}').format(add, output)
            print(cmd)
            subprocess.check_output(cmd, shell=True, universal_newlines=True)
        else: 
            print('Close time.... wait 30 second')
            time.sleep(30)
            
    def run(self):
        
        while True:
            try:
                self.recode()
            except Exception as e:
                if e == None:
                    e = 'Connection timed out'
                print('')
                print('error!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ', e)
                print('')
                pass
    
if __name__ == '__main__':
    def get_dvr_info(idx):
        df = pd.read_csv('/home/pi/workspace/stream/config.txt')
        shop_code = df.loc[idx, 'shop_code']
        acc = df.loc[idx, 'acc']
        pw = df.loc[idx, 'pw']
        ip = df.loc[idx, 'dvr_ip']
        port = df.loc[idx, 'port']
        ch = df.loc[idx, 'dvr_ch'] 
        ch_name = df.loc[idx, 'ch_name']
       
        return shop_code, acc, pw, ip, port, ch, ch_name
  
    config = pd.read_csv('/home/pi/workspace/stream/config.txt')
    config = config.sort_values('group', ascending = False)
    
    open_t = (6,0)
    close_t = (23,59)
    
    for i in config.index:
        shop_code, acc, pw, ip, port, ch, ch_name = get_dvr_info(i)
        main = video_recoding(shop_code, acc, pw, ip, port, ch, open_t, close_t, ch_name)
        main.start()
        time.sleep(1)
        

# -*- coding: utf-8 -*-
import paramiko
import pandas as pd
import os
import datetime
import glob
import threading
import time

class sendtoserver:
    
    def __init__(self, host, port, acc, pw, save_path):
        #threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.acc = acc
        self.pw = pw
        self.server_path = save_path
        
    def setData(self):
        
        self.local_path = '/home/pi/workspace/stream/save_video'
        self.file_name = self.get_filename()
        
    def get_filelist(self):
        
        files = os.listdir(self.local_path)
        condition = self.local_path + '/*.avi'
        files = glob.glob(condition)
        return files
    
    def send_sig(self, result):
        return result < datetime.datetime.now()
        
    def get_filename(self):
        
        self.files = ''
        self.file_name = ''
        self.files = self.get_filelist()
        
        for self.file_name in self.files:
            self.file_name = self.file_name.split('/')[-1]
            print(self.file_name)
            create_time = self.file_name[:12]
            year = create_time[:4]
            month = create_time[4:6]
            day = create_time[6:8]
            hour = create_time[8:10]
            _min = create_time[10:12]
            print(int(year), int(month), int(day), int(hour), int(_min))
            
            create_time = datetime.datetime(int(year), int(month), int(day), int(hour), int(_min))
            result = create_time + datetime.timedelta(seconds = 1860)
            print('result time : ', result)
            print('create_time : ', create_time)
            
            print(self.send_sig(result))
            
            if self.send_sig(result) == True:
                break
                
            else:
                time.sleep(1)
                self.files = ''
                self.file_name = ''
                self.files = self.get_filelist()
                pass
      
        return self.file_name     
    
    def create_log(self, local_path):
        paramiko.util.log_to_file(local_path + '/transfer.log')
    
    def transfer(self):
        self.file = ''        
        self.transport = paramiko.transport.Transport(host, port)
        
        while True:
            try:
                self.file = self.get_filename()
                print('file_name : ', self.file)
                print(self.transport.getpeername())
                self.transport.connect(username = self.acc, password = self.pw)
                self.sftp = paramiko.SFTPClient.from_transport(self.transport)
                self.sftp.put('/home/pi/workspace/stream/save_video/' + self.file, self.server_path + '/' + self.file)
                self.delete_file(self.file)
            except Exception as e:
                print('error : ', e)
                pass
            
            finally:
                self.sftp.close()
                self.transport.close()
                
                break
             
    def delete_file(self, file_name):
        os.remove('/home/pi/workspace/stream/save_video/' + file_name)
            
    def run(self):
        
        while True:
            try:
                self.setData()
                #self.create_log(self.local_path)
                self.transfer() 
                time.sleep(5)
            except Exception as e:
                print(e)
                pass
        
            
if __name__ == '__main__':
   
                
    host = '175.197.68.2'
    port = 23456
    acc = 'admin'
    pw = '@!Chaos123'
    #config = pd.read_csv('/home/pi/workspace/stream/config.txt')
    save_path = '/homes/brooks/DONJJANG/'
    

    main = sendtoserver(host, port, acc, pw, save_path)
    main.run()
    time.sleep(5)

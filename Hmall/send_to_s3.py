# -*- coding: utf-8 -*-
import pandas as pd
import os
import datetime
import glob
import threading
import time
import boto3
from botocore.exceptions import ClientError

class sendtoserver(threading.Thread):
    
    def __init__(self, shop_code, dvr_num, save_path, ch_name):
       
        threading.Thread.__init__(self)
        
        self.shop_code = shop_code
        self.dvr_num = dvr_num
        self.ch_name = ch_name
        self.server_path = save_path
        
    def setData(self):
        #self.now = datetime.datetime.now()
        #self.today = self.now.strftime('%Y%m%d')
        #self.work_dir = os.getcwd() + '/'
        self.local_path = os.path.join('/home/qtumai/workspace/stream/save_video/', str(self.dvr_num), self.ch_name)
        print('local_path : ', self.local_path)
        self.file_name = self.get_filename()
        
        self.access_key_id = "AKIAQXXRPHPXI4CQSHEX"
        self.secret_key = "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"
        self.bucket_name = "qtumaitest"
        while True:
            try:
                self.response = self.create_bucket(self.bucket_name)
                break
            except Exception as e:
                print(e)
                continue
        self.s3 = boto3.client("s3", aws_access_key_id = self.access_key_id, aws_secret_access_key = self.secret_key)
        
    def get_filelist(self):
        
        files = os.listdir(self.local_path)
        condition = self.local_path + '/*.mkv'
        files = glob.glob(condition)
        return sorted(files)
    
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
            #print(int(year), int(month), int(day), int(hour), int(_min))
            
            create_time = datetime.datetime(int(year), int(month), int(day), int(hour), int(_min))
            result = create_time + datetime.timedelta(seconds = 1860)
            
            
            print(self.send_sig(result))
            
            if self.send_sig(result) == True:
                print('select file : ', self.file_name)
                break
                
            else:
                time.sleep(1)
                self.files = ''
                self.file_name = ''
                self.files = self.get_filelist()
                pass
      
        return self.file_name     
    #채널 분류
    def ch_classification(self):
        self.group = ''
        tmp = ''
        tmp = self.ch_name[-2:]
        entrance = ['01','04']
        etc = ['03','05','06', '07']
        out = ['02','08']
        
        if tmp in entrance:
            self.group = 'entrance'
            
        elif tmp in etc:
            self.group = 'etc'
            
        elif tmp in out:
            self.group = 'out'
        else:
            print(tmp + '채널 설정확인')
            
    def delete_file(self):
        os.remove(self.local_path + '/' + self.file)
       
    def create_bucket(self, bucket_name):
        print("Creating a bucket..." + bucket_name)
        s3 = boto3.client("s3", aws_access_key_id = self.access_key_id, aws_secret_access_key = self.secret_key)
        try:
            response = s3.create_bucket(Bucket = bucket_name, CreateBucketConfiguration = {"LocationConstraint": "ap-northeast-2"})
            return response
        except ClientError as e:
            if e.response['Error']['Code'] == "BucketAlreadyOwnedByYou":
                print("Bucket already exists. skipping..")
            else:
                print("Unknown error, exit...")    
        
    def transfer(self):
        try:
            self.file = ''
            self.file = self.get_filename()
            print('file : ', self.file)
            #self.shop_code = self.file.split('_')[2]
            self.ch_classification()
        
            print('file upload : %s       strat time : %s' % (self.file, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            #print('file_path : ', self.local_path)
            self.s3.upload_file(self.local_path + '/' + self.file, self.bucket_name, 'HMALL/' + self.shop_code + '/' + self.group + '/' + self.file)
            print('file upload : %s       finish time : %s' % (self.file, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            #print('upload path : ' + 'Validation/' + self.file)
            #self.s3.upload_file(self.local_path + '/' + self.file, self.bucket_name, 'Validation/' + self.file)
            
            #print('upload finish : ', self.file)
            self.delete_file()
        except Exception as e:
            print(e)
            time.sleep(10)
    
    def run(self):
        
        while True:
            self.setData()
            
            #self.create_log(self.local_path)
            self.transfer() 
            
            time.sleep(0.5)
        
            
if __name__ == '__main__':
    
    def get_dvr_info(idx):
        df = pd.read_csv('/home/qtumai/workspace/stream/config.txt')
        
        #dvr_ip = df.loc[idx, 'dvr_ip']
        #dvr_ch = df.loc[idx, 'dvr_ch'] 
        shop_code = df.loc[idx, 'shop_code']
        dvr_num = df.loc[idx, 'dvr_num']
        ch_name = df.loc[idx, 'ch_name']
        
        return shop_code, dvr_num, ch_name
   
                
  
    config = pd.read_csv('/home/qtumai/workspace/stream/config.txt')
    save_path = '/homes/qtumai/workspace/stream/save_video/'
    
    for i in config.index:
        shop_code, dvr_num, ch_name = get_dvr_info(i)
        main = sendtoserver(shop_code, dvr_num, save_path, ch_name)
        main.start()
        time.sleep(0.01)    
    '''   
    shop_code, dvr_num, ch_name = get_dvr_info(0)
    main = sendtoserver(shop_code, dvr_num,  save_path, ch_name)
    main.start()
    time.sleep(0.5)    
    '''
    

import time
import os
import re
import json
import urllib
import requests as rq
import schedule
import pymysql
import sqlite3
from hdfs.client import Client
from ftplib import FTP
import configparser as cp
import cv2

class Conf:
    def __init__(self):
        self.config = cp.ConfigParser()
        self.config.read('ftu.conf')
        
    def get_db_conf(self):
        return self.config['db']
    
    def get_ftp_conf(self):
        return self.config['ftp']
    
    def get_hdfs_conf(self):
        return self.config['hdfs']
    
    def get_prometheus_conf(self):
        return self.config['prometheus']
    
    def get_localdb_conf(self):
        return self.config['localdb']

conf = Conf()

class DBUtils:
    def __init__(self):
        config = conf.get_db_conf()
        self.conn = pymysql.connect(config['db_host'], config['db_user'], config['db_pass'], config['db_name'])
        
    def insert(self, tb_name, kvs):
        cursor = self.conn.cursor()
        id = None
        try:
            key_items = str(tuple(kvs.keys()))
            key_items = key_items.replace('\'', '')
            value_items = str(tuple(kvs.values()))
            sql_query = 'insert into '+tb_name+' '+key_items+' values '+value_items
            cursor.execute(sql_query)
            self.conn.commit()
            id = cursor.lastrowid
        except Exception as e:
            print('query error!{}'.format(e))
        finally:
            cursor.close()
        return id
    
class LocalDBUtuils:
    def __init__(self):
        config = conf.get_localdb_conf()  
        self.db_name = config['db_name']
        self.tb_name = 'ftp_file'
        self.conn = sqlite3.connect(self.db_name)
        self.create_table()
        self.create_index('PATH')
        
    def create_index(self, col):
        cursor = self.conn.cursor()
        try:
            cursor.execute('create index path_index on '+self.tb_name+' ('+col+')')
            self.conn.commit()
        except:
            pass
    def insert(self, kvs):
        cursor = self.conn.cursor()
        try:
            key_items = str(tuple(kvs.keys()))
            key_items = key_items.replace('\'', '')
            value_items = str(tuple(kvs.values()))
            sql_query = 'insert into '+self.tb_name+' '+key_items+' values '+value_items
            cursor.execute(sql_query)
            self.conn.commit()
            id = cursor.lastrowid
        except Exception as e:
            print('query error!{}'.format(e))
        finally:
            cursor.close()
        return id
    
    def create_table(self):
        cursor = self.conn.cursor()
        sql_str = 'CREATE TABLE '+self.tb_name+\
                    ' (ID INTEGER PRIMARY KEY AUTOINCREMENT,\
                    NAME TEXT NOT NULL,\
                    PATH TEXT NOT NULL);'
        try:
            cursor.execute(sql_str)
            self.conn.commit()
        except:
            pass
        
    def query(self, path):
        cursor = self.conn.execute('SELECT * FROM '+self.tb_name+' where PATH='+path)
        return True if len(cursor.fetchall())>0 else False
        
class HDFSUtils:
    def __init__(self):
        config = conf.get_hdfs_conf()
        self.hdfs_host = config['hdfs_host']
        self.hdfs_port = config['hdfs_port']
        self.host_dict = eval(config['hdfs_slaves'])
        hdfs_address = "http://"+self.hdfs_host+":"+str(self.hdfs_port)
        self.client = Client(hdfs_address)
        
    def upload(self,hdfs_path, video_file):
        print('uploading file ',video_file)
        ret = self.client.upload(hdfs_path, video_file)
        print(ret)
        
    def query(self, path):
        path = "http://"+self.hdfs_host+":"+str(self.hdfs_port)+"/webhdfs/v1"+urllib.parse.quote(path)+"?op=OPEN"
        print(path)
        ret = rq.get(path, allow_redirects = False)
        if(ret.status_code == 307):
            download_link = ret.headers['Location']
            for item in self.host_dict.keys():
                download_link = download_link.replace("http://"+item, "http://"+self.host_dict[item])
        else:
            print(ret.text)
        return download_link
    
class Upload:
    def __init__(self):
        self.db_utils = DBUtils()
        self.hdfs_utils = HDFSUtils()
   
    def set_work_path(self, path):
        self.work_path = os.path.abspath(path)
        return self
        
    def upload(self):
        self.search(self.work_path)
        
    def get_video_info(self, path, property):
        cap = cv2.VideoCapture(path)
        property['fps'] = round(cap.get(cv2.CAP_PROP_FPS))
        property['duration'] = float(cap.get(cv2.CAP_PROP_FRAME_COUNT))/float(cap.get(cv2.CAP_PROP_FPS))
        property['format'] = cap.get(cv2.CAP_PROP_FORMAT)
        property['codec'] = cap.get(cv2.CAP_PROP_FOURCC)
        property['bit_rate'] = cap.get(cv2.CAP_PROP_FRAME_WIDTH)
        property['width'] = cap.get(cv2.CAP_PROP_FRAME_WIDTH)
        property['height'] = cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
        cap.release()
        
    def insert_db(self, path, video_file, json_file, task_id):
        with open(json_file, "r") as f:
            content = f.read()
        property = {}
        try:
            property = json.loads(content)
        except Exception as e:
            print(e)
            
        property['hdfs_address'] = path
        property['size'] = os.path.getsize(video_file)
    #    property['load_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        property['load_time'] = time.strftime("%Y-%m-%d", time.localtime())
        property['task_id'] = task_id
        
        if len(property['keywords'])==0:
            property['keywords'] = ''
        else:
            property['keywords'] = property['keywords'].replace('\'','')
            property['keywords'] = property['keywords'].replace('[','')
            property['keywords'] = property['keywords'].replace(']','')
            
        property.pop('video_time')
        property.pop('site_name_cn')
        property.pop('info_cn')
        
        self.get_video_info(video_file, property)
        return self.db_utils.insert('tb_video_origin', property)
    
    def add_task(self, json_file):
        
        with open(json_file, "r") as f:
            content = f.read()

        property = {}
        try:
            property = json.loads(content)
        except Exception as e:
            print(e)
        property['task_title']=property['title']
        property['task_time']=property['time']
        property['task_begin']=property['time']
        property['task_end']=property['time']
        property['task_size']=property['file_size']
        property['task_key']=property['keywords']
        property['task_number']=property['file_number']
        
        property.pop('title')
        property.pop('time')
        property.pop('file_size')
        property.pop('keywords')
        property.pop('file_number')
        return self.db_utils.insert('tb_task',property)
        
    def query(self, path):
        path = "http://"+self.hdfs_host+":"+str(self.hdfs_port)+"/webhdfs/v1"+urllib.parse.quote(path)+"?op=OPEN"
        ret = rq.get(path, allow_redirects = False)
        if(ret.status_code == 307):
            download_link = ret.headers['Location']
            for item in self.host_dict.keys():
                download_link = download_link.replace("http://"+item, "http://"+self.host_dict[item])
        else:
            raise FileNotFoundError
        return download_link
    
    def prometheus(self, id):
        params = {'start':str(id), 'end':str(id)}
        ret = rq.get('http://10.168.103.200:5000/videoload', params = params)
        print(ret.text)
         
    def search(self, localpath):
        dirs = os.listdir(localpath)
        files = []
        for dir in dirs:
            if os.path.isdir(os.path.join(localpath, dir)):
                self.search(os.path.join(localpath, dir))
            else:
                files.append(dir[:dir.rfind('.')])
        if(len(files)):
            print('uploading files from path ', localpath)
            task_file = os.path.join(localpath, 'task_info.json')
            task_id = 0
            try:
                task_id = self.add_task(task_file)
            except Exception as e:
                print("no task_file was found")
            else:
                for file in set(files):
                    file = os.path.join(localpath, file)
                    if(os.path.exists(file+'.json') and (os.path.exists(file+'.mp4') or 
                                                         os.path.exists(file+'.webm'))):
                        video_file = file
                        if os.path.exists(file+'.mp4'):
                            video_file +='.mp4'
                        else:
                            video_file += '.webm'
                        hdfs_path = file[len(self.work_path):]
                        download_link = ''
                        try:
                            self.hdfs_utils.upload(hdfs_path, video_file)
                        except Exception as e:
                            pass
                        try:
                            download_link = self.hdfs_utils.query(hdfs_path)
                            ret  = urllib.parse.urlparse(download_link)
                            path = urllib.parse.unquote(urllib.parse.urlunparse((ret.scheme, ret.netloc, ret.path, '', '', ret.fragment)))
                            id = self.insert_db(path, video_file, json_file = file+'.json', task_id = task_id)
                            self.prometheus(id)
                            os.remove(video_file)
                            os.remove(file+'.json')
                        except:
                            print('file not found')
                            pass
                
class FileTransfer:
    def __init__(self):
        config = conf.get_ftp_conf()
        self.ftp_host = config['ftp_host']
        self.ftp_port = config.getint('ftp_port')
        self.ftp_user = config['ftp_user']
        self.ftp_pass = config['ftp_pass']
        self.ftp = FTP()
        self.local_db = LocalDBUtuils()
        
        try:
            self.ftp.connect(self.ftp_host, self.ftp_port, timeout=10)  # ftp  connect函数的作用
            self.ftp.encoding='utf-8'
            print (self.ftp.welcome)
        except Exception as e:
            print ("error: can not connect to '%s' %s").decode("utf-8") % (self.ftp_host, e)
            exit()
        try:
            self.ftp.login(user=self.ftp_user, passwd=self.ftp_pass)
        except Exception as e:
            print ("error: invalid username or password")
            exit()
        self.file_list = []
    
    def find_files(self):
        self.file_list = []
        try:
            name_list = self.ftp.nlst()
            self.ftp.dir(self.get_file_list)
            for i in range(len(self.file_list)):
                self.file_list[i][1] = name_list[i]
        except Exception as e:
            print (e)
        return self.file_list
    
    def get_filename(self, line):
        items = re.split(' +', line)
        file_arr = [line[0], ' '.join(items[8:])]
        return file_arr
    
    def get_file_list(self,line):
        file_arr = self.get_filename(line)
        if file_arr[1] not in ['.', '..']:
            self.file_list.append(file_arr)
            
    def file_valid(self, filename):
        print("testing file ", filename)
        size1 = self.ftp.size(filename)
        time.sleep(3)
        size2 = self.ftp.size(filename)
        return size1 == size2
                
    def download_file(self, path, remotepath, filename):
        if not os.path.isdir(path):  
            os.makedirs(path)
        if(self.file_valid(filename)):
            file_size = self.ftp.size(filename)
            
            print("downloading file")
            fp = open(path+'/'+filename, 'wb')
            while file_size != fp.tell():
                bufsize = 1024
                try:
                    self.ftp.retrbinary('RETR ' + filename, fp.write, bufsize, rest=fp.tell())
                    self.ftp.set_debuglevel(0)
                except Exception as e:
                    self.ftp.connect(self.ftp_host, self.ftp_port, timeout=10)
                    self.ftp.login(user=self.ftp_user, passwd=self.ftp_pass)
                    self.ftp.cwd(remotepath)
            fp.close()
        
    def transfer_files(self, remotepath, localpath):
        print('current path ', remotepath)
        self.ftp.cwd(remotepath)
        file_list = self.find_files()
        try:
            for fileitem in file_list:
                filetype = fileitem[0]
                filename = fileitem[1]
                if filetype == 'd':
                    self.transfer_files(os.path.join(self.ftp.pwd(),filename), localpath+'/'+filename)
                    self.ftp.cwd('..')
                elif filetype=='-':
                    if not self.exists(self.ftp.pwd(), filename):
                        self.download_file(localpath, remotepath, filename)
                        print('downloaded.')
                        self.update(self.ftp.pwd(), filename)
        except Exception as e:
            print (e)
        return self
    
    def exists(self, ftp_path, filename):
        file = os.path.join(ftp_path, filename)
        file = file.replace('\'', '\'\'')
        file = '\''+file+'\''
        return self.local_db.query(file)
    
    def update(self, ftp_path, filename):
        kvs={'NAME':filename, 'PATH':os.path.join(ftp_path, filename)}
        self.local_db.insert(kvs)
    
    def delete_file(self, remotefile):
        self.ftp.delete(remotefile)
        
    def wait(self, sec):
        time.sleep(sec)
        return self

    def report(self):
        time_now = time.localtime()
        print("%d-%d-%d %02d:%02d:%02d"%(time_now[0],time_now[1],time_now[2],time_now[3],time_now[4],time_now[5]))
        return self
    
class Worker:
    def __init__(self, local_path, remote_path):
        self.local_path = local_path
        self.remote_path = remote_path
        self.ft = FileTransfer()
        self.ul = Upload()
   
    def download(self):
        self.ft.report().transfer_files(localpath = self.local_path, remotepath=self.remote_path) 
    
    def upload(self):
        self.ul.set_work_path(self.local_path).upload()
    
    def work(self):
        # self.download()
        self.upload()
        
def run():
    # local_path = "/home/kou/000wt/videos/ftp/国外首脑"
    local_path = "/home/kou/data/dealt_dir"
    remote_path = "/home/ubuntu/videos/"
    worker = Worker(local_path, remote_path)
    worker.work()
    
if __name__ == '__main__':
    #schedule.every(1).day.at('14:00').do(run)
    #while(True):
    #    schedule.run_pending()
    run()
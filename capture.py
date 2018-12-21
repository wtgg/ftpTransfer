import imageio
import cv2
import requests as rq
import urllib
#reader = imageio.get_reader('/home/usr02/data/ftp/youtube/CBSNewsOnline/2018-11-26/4 people dead, including gunman, in hospital shooting in Chicago.mp4')
#print(reader.get_meta_data())

#vc = cv2.VideoCapture('/home/usr02/data/ftp/youtube/Blockchain/2018-12-05/What is Blockchain.mp4')
#vc.get(cv2.CAP_PROP_FPS)

path = '/Blockchain/2018-12-04/oddity OPEN #3 - Blockchain (Trailer)'
hdfs_host = '10.168.103.101'
hdfs_port = 50070
path = "http://"+hdfs_host+":"+str(hdfs_port)+"/webhdfs/v1"+urllib.parse.quote(path)+"?op=OPEN"
print(path)
ret = rq.get(path, allow_redirects=False)
if(ret.status_code == 307):
    download_link = ret.headers['Location']
else:
    raise FileNotFoundError
print(download_link)

ret  = urllib.parse.urlparse(download_link)
path = urllib.parse.urlunparse((ret.scheme, ret.netloc, ret.path, '', '', ret.fragment))

path = urllib.parse.unquote(path)
print(path)                            

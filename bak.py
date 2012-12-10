#! /usr/bin/env python
#coding=utf-8

##爬整站的爬虫，有多深爬多深，可能坠入万丈深渊永不复返，但不曾悔过
##by:lu4nx [lx@shellcodes.org]
##2012.12.07

import urllib2 as urllib
import mysql
import optparse,threading,Queue,urlparse,time,os,sys,re,gzip,StringIO
#threading._VERBOSE=True

urlQueue = Queue.Queue()
outQueue = Queue.Queue()
globalLock = threading.RLock()
#visited = []		#记录已爬过的url
threadsPool = []	#线程池
#自定义HTTP头
customHeaders = {#'User-Agent':'by:lu4nx',
				 'Connection':'keep-alive',
				 'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.91 Safari/537.11',
				 'Accept-Encoding':'gzip,deflate,sdch',
				 'Accept-Language:':'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4',
				 'Accept-Charset':'GBK,utf-8;q=0.7,*;q=0.3',
				 'Referer':'http://www.baidu.com/s?wd=lu4nx',
				}
	
class Spider(object):
	def __init__(self):
		self.domain = None
		self.visited = []		#记录已爬过的url

	def fetchUrls(self,threadId):
		"""从队列中取url。直到urlQueue为空，超时后产生Queue.Empty异常并结束线程"""
		while True:
			try:
				url = urlQueue.get(timeout=10)
				urlQueue.task_done()
			except Queue.Empty:
				print u'线程%s结束'%threadId
				break

			#记录此url为已爬过。在urlopen之前就做记录，如果有重复但不能正常访问的url，免去徒劳的urlopen
#			globalLock.acquire()

#			if url in self.visited:
#				 globalLock.release()
#				 continue
#			else: globalLock.release()

			if url in self.visited: continue

			try:
				urlRequest = urllib.Request(url,headers=customHeaders)
				sock = urllib.urlopen(urlRequest,timeout=20)
#				globalLock.acquire()
				self.visited.append(url)
#				globalLock.release()
				htmlCode = sock.read()
			except Exception,err:
				print u'访问%s出错，详细信息：\n%s'%(url,err)
				continue

			#对Gzip压缩数据进行解压
			try:
				gzipObj = gzip.GzipFile(fileobj=StringIO.StringIO(htmlCode),mode='r')
				htmlCode = gzipObj.read()
			except Exception,err:
				htmlCode = gzipObj.extrabuf

			#处理两种情况：1.重定向;2.url=http://test/x,如果是目录,url=http://test/x/, 否则url=http://test/x
			realUrl = sock.geturl()
			sock.close()

			urls = re.findall(r"<a\s+href\s*=\s*[\"']*([^\"'\s]+)[\"']*[^<]+</a>",htmlCode)

			#抓取图片src、script src、css src、link src
			#抓取外部脚本
			scriptFile = re.findall(r"<script\s.*src\s*=\s*[\"']*([^\"'\s]+).*",htmlCode)

			#if scriptFile:
			for _ in scriptFile:
				outQueue.put(_)

			inPagelinks = []

			for link in urls:
				if link == None: continue
				elif link.find('#') != -1 or link[0:11] == ('javascript:'): continue
				realLink = link
				link = urlparse.urljoin(realUrl,link)
				#同一个站点的url才保存
#				if self._isSomeDomain(link):inPagelinks.append(link)
				if self._isSameSite(link): 
					inPagelinks.append(link)
					outQueue.put(realLink)

			#过滤掉重复的
			set(inPagelinks)

			for url in inPagelinks: urlQueue.put(url)

	def _isSomeSite(self,url):
		""" 删除其他站点的链接，如:www.xxx.com，页面中出现d1.xxx.com，将一并爬行 """
		if self._isSameSite(url): return True
		else: return False

	def _isSomeDomain(self,url):
		"""删除其他域的超链接，如:www.xxx.com，页面中包含t1.xxx.com链接，后者将不会爬行"""
		if self.getUrlDomain(url) == self.domain: return True
		else: return False

	def _isSameSite(self,url):
		"""判断url是否和当前爬虫工作所在url是来自同一站点"""
		#www.xxx.com -> .xxx.com
		domain1 = self.domain[self.domain.find('.'):]
		domain2 = self.getUrlDomain(url)
		domain2 = domain2[domain2.find('.'):]

		if domain2 != domain1: return False
		else: return True 

	def getUrlDomain(self,destUrl):
		"""
		>>> Spider().getUrlDomain('http://xxx.com/test')
		'xxx.com'
		"""
		return  urlparse.urlparse(destUrl).netloc

class URL(object):
	def __init__(self):
		self.urls = []
		self.dbName = None
		self.db = mysql.DB()

	def fetchUrlsFromQueue(self,dbName):
		self.dbName = dbName.replace('.','_')
		while True:
			try:
				url = outQueue.get(timeout=20)
				outQueue.task_done()
			except Queue.Empty:
				print u'url处理线程结束'
				break

			if url not in self.urls:
				self.urls.append(url)
				print url
				#self.saveUrlToDb(url)

	def saveUrlToDb(self):
		#当前日期
		currentDate = time.strftime('%Y-%m-%d') 

		print u'开始存储到数据库中...'
		for url in self.urls:
			self.db.insert(self.dbName,url,currentDate)

def main(rootUrl,threadCount):
	spider = Spider()
	spider.domain = spider.getUrlDomain(rootUrl)

	#插入根url到队列
	urlQueue.put(rootUrl)

	#创建爬虫的线程池
	for i in range(threadCount):
		thread = threading.Thread(target=spider.fetchUrls,args=(i,))
		thread.daemon = True
		threadsPool.append(thread)
		thread.start()

	#创建url处理线程
	urlObj = URL()
	urlThread = threading.Thread(target=urlObj.fetchUrlsFromQueue,args=(spider.domain,))
	urlThread.daemon = True
	urlThread.start()
	
	#等待爬虫线程的结束
	for _ in threadsPool: _.join()
	print u'完成url爬取'

	#等待url处理线程结束
	urlThread.join()

	#将链接存储到数据库中
	urlObj.saveUrlToDb()
	print u'全部链接已存入数据库'

if __name__ == '__main__':
	opt = optparse.OptionParser(version='0.1',
				    usage='spider.py -u http://xxx --thread [thread count]', description='my spider,by:lu4nx')

	opt.add_option('-u',help='url',dest='rootUrl')
	opt.add_option('--thread',help=u'线程数，默认10',dest='threadCount',type='int')
	opt.set_defaults(threadCount=10)

	(opt,args) = opt.parse_args()

	try:
		main(opt.rootUrl,opt.threadCount)
	except Exception,err: print u'参数错误，请加参数-h获得帮助信息';print err

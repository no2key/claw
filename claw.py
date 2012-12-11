#! /usr/bin/env python
#coding=utf-8

##爬整站的爬虫，有多深爬多深，可能坠入万丈深渊永不复返，但不曾悔过
##by:lu4nx [lx@shellcodes.org]
##2012.12.07

import urllib2 as urllib
import db
import optparse,threading,Queue,urlparse,time,os,sys,re,gzip,StringIO
#threading._VERBOSE=True	#线程调试

urlQueue = Queue.Queue()
outQueue = Queue.Queue()
threadsPool = []
#自定义HTTP头
customHeaders = {#'User-Agent':'by:lu4nx',
				 'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.91 Safari/537.11',
				 'Accept-Language:':'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4',
				 'Accept-Encoding':'gzip,deflate',
				 'Accept-Charset':'GBK,utf-8;q=0.7,*;q=0.3',
				 'Connection':'keep-alive',
				 'Referer':'http://www.baidu.com/s?wd=lu4nx',
				}

class DownloadPage(object):
	'''下载网页代码'''
	def __init__(self):
		self.sock = None

	def fetchHtmlCode(self,url,numOfRetries=3):
		'''下载网页的代码,numOfRetries是超时后重试次数'''
		try:
			urlRequest = urllib.Request(url,headers=customHeaders)
			self.sock = urllib.urlopen(urlRequest,timeout=20)
			return self.extractHTML(self.sock.read())
		except Exception,err:
			print u'access %s error,info:\n%s'%(url,err)
#			time.sleep(1)	#为了解决加速乐防cc
			#请求失败后的重试机制
			if numOfRetries <= 0:
		#		print u'重试失败'
				return None
			else:
		#		print u'重试%s次'%(numOfRetries-1)
				self.fetchHtmlCode(url,numOfRetries-1)

	def extractHTML(self,htmlCode):
		'''解压Gzip压缩过的HTML代码'''
		try:
			gzipObj = gzip.GzipFile(fileobj=StringIO.StringIO(htmlCode),mode='r')
			return gzipObj.read()
		except:
			return gzipObj.extrabuf

	def getRealUrl(self):
		'''返回由服务器返回的真实url'''	
		return self.sock.geturl()

	def close(self):
		'''关闭socket连接'''
		self.sock.close
	
class Spider(object):
	def __init__(self):
		self.domain = None
		self.visited = []		#记录已爬过的url

	def spider(self,threadId):
		'''从队列中取url。直到urlQueue为空，超时后产生Queue.Empty异常并结束线程'''
		downPageObj = DownloadPage()

		while True:
			try:
				url = urlQueue.get(timeout=10)
				urlQueue.task_done()
			except Queue.Empty:
				print u'kill thread%s'%threadId
				break

			if url in self.visited: continue
			#在对url发起连接的之前就把这个url保存起来，如果无法打开这个url，再遇到这个url时就不用再去连接一次了
			else:self.visited.append(url)

			htmlCode = downPageObj.fetchHtmlCode(url)

			if htmlCode == None:continue

			#处理两种情况：1.重定向;2.url=http://test/x,如果是目录,url=http://test/x/, 否则url=http://test/x
			realUrl = downPageObj.getRealUrl()
			downPageObj.close()

			urls = self.fetchUrls(htmlCode)

			#将其他资源链接保存起来（图片、js、css等）
			otherRes = self.fetchOtherResource(htmlCode)

			for rec in otherRes:outQueue.put(rec)

			inPagelinks = []

			for link in urls:
				if link == None: continue
				elif link.find('#') != -1 or link[0:11] == ('javascript:'): continue
				realLink = link
				link = urlparse.urljoin(realUrl,link)
				#同一个站点的url才保存
#				if self._isSomeDomain(link):
				if self._isSameSite(link): 
					inPagelinks.append(link)
					outQueue.put(realLink)

			#过滤掉重复的
			set(inPagelinks)

			for url in inPagelinks: urlQueue.put(url)

	def _isSomeSite(self,url):
		'''删除其他站点的链接，如:www.xxx.com，页面中出现d1.xxx.com，将一并爬行'''
		if self._isSameSite(url): return True
		else: return False

	def _isSomeDomain(self,url):
		'''删除其他域的超链接，如:www.xxx.com，页面中包含t1.xxx.com链接，后者将不会爬行'''
		if self.getUrlDomain(url) == self.domain: return True
		else: return False

	def _isSameSite(self,url):
		'''判断url是否和当前爬虫工作所在url是来自同一站点'''
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

	def fetchOtherResource(self,htmlCode):
		'''抓取图片、js、css等链接'''
		otherResource = re.findall(r"<script\s.*src\s*=\s*[\"']*([^\"'\s]+).*",htmlCode)
		otherResource += re.findall(r"<link.*href\s*=\s*[\"']*([^\"'\s]+).*",htmlCode)
		otherResource += re.findall(r"<img\s.*src\s*=[\"']*([^\"'\s]+).*",htmlCode)
		return otherResource

	def fetchUrls(self,htmlCode):
		'''从html代码中提取超链接'''
		return re.findall(r"<a\s+href\s*=\s*[\"']*([^\"'\s]+)[\"']*[^<]+</a>",htmlCode)

class StoreURI(object):
	def __init__(self,tableName):
		self.urls = []
		self.tableName = tableName.replace('.','_')
		self.db = db.DB(self.tableName)

	def store(self):
		while True:
			try:
				url = outQueue.get(timeout=3)
				outQueue.task_done()
			except Queue.Empty:
				continue

			if url == 'kill you,by:lu4nx':break

			if url not in self.urls:
				self.urls.append(url)
				print url
				self.storeUrlToDb(url)

	def storeUrlToDb(self,url):
		#当前日期
		currentDate = time.strftime('%Y-%m-%d') 
		self.db.insert(self.tableName,url,currentDate)

def main(rootUrl,threadCount):
	spider = Spider()
	spider.domain = spider.getUrlDomain(rootUrl)

	#插入根url到队列
	urlQueue.put(rootUrl)

	#创建爬虫的线程池
	for i in range(threadCount):
		thread = threading.Thread(target=spider.spider,args=(i,))
		thread.daemon = True
		threadsPool.append(thread)
		thread.start()

	#创建存储uri的线程
	urlObj = StoreURI(spider.domain)
	urlThread = threading.Thread(target=urlObj.store)
	urlThread.daemon = True
	urlThread.start()
	
	#等待爬虫线程的结束
	for _ in threadsPool: _.join()

	#通知存储线程结束
	outQueue.put('kill you,by:lu4nx')

	#等待url处理线程结束
	urlThread.join()
	print u'完成url爬取'

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

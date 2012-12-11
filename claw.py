#! /usr/bin/env python
#coding=utf-8

##爬整站的爬虫，有多深爬多深，可能坠入万丈深渊永不复返，但不曾悔过
##by:lu4nx [lx@shellcodes.org]
##2012.12.07

import urllib2 as urllib
import db
import optparse,threading,Queue,urlparse,time,os,sys,re,gzip,StringIO,hashlib
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
			time.sleep(1)	#为了解决加速乐防cc
			urlRequest = urllib.Request(url,headers=customHeaders)
			self.sock = urllib.urlopen(urlRequest,timeout=20)
			return self.extractHTML(self.sock.read())
		except Exception,err:
			print u'access %s error,info:\n%s'%(url,err)
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

			#md5加密url，固定长度，节约内存
			if hashlib.md5(url).hexdigest() in self.visited: continue
			#在对url发起连接的之前就把这个url保存起来，如果无法打开这个url，再遇到这个url时就不用再去连接一次了
			else:self.visited.append(hashlib.md5(url).hexdigest())

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
#				if self.isSameDomain(link):
				#同一个站点的url才保存
				if self.isSameSite(link): 
					inPagelinks.append(link)
					outQueue.put(realLink)

			#过滤掉重复的
			set(inPagelinks)

			for url in inPagelinks: urlQueue.put(url)

	def isSameDomain(self,url):
		"""判断url的域名与所爬网站的域名是否相等
		"""
		if self.getUrlDomain(url) == self.domain: return True
		else: return False

	def isSameSite(self,url):
		'''判断url是否与所爬的url来自同一站点'''
		#www.xxx.com -> .xxx.com
		domain1 = self.domain[self.domain.find('.'):]
		domain2 = self.getUrlDomain(url)
		domain2 = domain2[domain2.find('.'):]

		if domain2 != domain1: return False
		else: return True 

	def getUrlDomain(self,destUrl):
		"""
		>>> spider = Spider()
		>>> spider.domain = 'lx.shellcodes.org'
		>>> spider.getUrlDomain('http://xxx.com/test')
		'xxx.com'
		>>> spider.getUrlDomain('lx.shellcodes.org')
		'lx.shellcodes.org'
		"""
		if destUrl == self.domain: return destUrl
		else: return urlparse.urlparse(destUrl).netloc

	def fetchOtherResource(self,htmlCode):
		"""抓取图片、js、css等链接
		>>> spider = Spider()
		>>> spider.domain = 'lx.shellcodes.org'
		>>> htmlCode = '<img src="http://www.baidu.com/logo.png">'
		>>> spider.fetchOtherResource(htmlCode)	
		[]
		>>> htmlCode =  '<img src="http://lx.shellcodes.org/logo.png">'
		>>> spider.fetchOtherResource(htmlCode)
		['http://lx.shellcodes.org/logo.png']
		>>> htmlCode =  '<img src="http://lx.shellcodes.org/logo.png" />'
		>>> spider.fetchOtherResource(htmlCode)
		['http://lx.shellcodes.org/logo.png']
		>>> htmlCode =  '<IMG src="http://lx.shellcodes.org/logo.png" />'
		>>> spider.fetchOtherResource(htmlCode)
		['http://lx.shellcodes.org/logo.png']
		"""
		otherResource = []
		scriptTags = re.findall(r"(?i)<script\s.*src\s*=\s*[\"']*([^\"'\s]+).*",htmlCode)
		linkTags = re.findall(r"(?i)<link.*href\s*=\s*[\"']*([^\"'\s]+).*",htmlCode)
		imgTags = re.findall(r"(?i)<img\s.*src\s*=[\"']*([^\"'\s]+).*",htmlCode)

		#不需要来自其他网站的外部链接
		for src in scriptTags+linkTags+imgTags:
			srcURL = urlparse.urlparse(src)

			#如果链接里有域名，就看是不是来自自己网站的
			if srcURL.netloc == '': otherResource.append(src)
			elif self.isSameDomain(srcURL.netloc): otherResource.append(src)
				
		return otherResource

	def fetchUrls(self,htmlCode):
		'''从html代码中提取超链接'''
		return re.findall(r"(?i)<a\s+href\s*=\s*[\"']*([^\"'\s]+)[\"']*[^<]+</a>",htmlCode)

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

	#等待uri存储线程结束
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

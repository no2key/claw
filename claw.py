#! /usr/bin/env python
#coding=utf-8

##爬整站的爬虫，有多深爬多深，可能坠入万丈深渊永不复返，但不曾悔过
##by:lu4nx [lx@shellcodes.org]
##2012.12.07

import urllib2 as urllib
import optparse,threading,Queue,urlparse,time,os,sys,re,gzip,StringIO,hashlib,logging,cPickle
import db
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

#日志文件
logFile = 'claw_log.log'
#配置日志
log = logging.getLogger('claw')
#记录请求失败的网站
urlForOpenError = logging.getLogger('urlerror')
logging.basicConfig(filename=logFile,filemode='w',format='%(name)s|%(levelname)s|%(asctime)s|%(threadName)s|%(message)s')

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
			#请求失败后的重试机制
			if numOfRetries <= 0:
				#把重试失败的记录写到日志文件中
				urlForOpenError.warning('%s|%s'%(url,err))
				return None
			else:self.fetchHtmlCode(url,numOfRetries-1)

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
	'''爬虫主类，从url队列中不断获得url进行爬取'''
	def __init__(self):
		self.domain = None
		self.visited = []		#记录已爬过的url

	def spider(self):
		'''从队列中取url。直到urlQueue为空，超时后产生Queue.Empty异常并结束线程'''
		downPageObj = DownloadPage()

		while True:
			try:
				url = urlQueue.get(timeout=10)
				urlQueue.task_done()
			except Queue.Empty:
				print 'kill %s'%threading.currentThread().getName()
#				log.debug('kill %s'%threading.currentThread().getName())
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
			otherUris= self.fetchOtherUris(htmlCode)

			for uri in otherUris:outQueue.put(uri)

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
		'''判断url的域名与所爬网站的域名是否相等'''
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

	def fetchOtherUris(self,htmlCode):
		"""抓取图片、js、css、flash、iframe等链接
		>>> spider = Spider()
		>>> spider.domain = 'lx.shellcodes.org'
		>>> htmlCode = '<img src="http://www.baidu.com/logo.png">'
		>>> spider.fetchOtherUris(htmlCode)	
		[]
		>>> htmlCode =  '<img src="http://lx.shellcodes.org/logo.png">'
		>>> spider.fetchOtherUris(htmlCode)
		['http://lx.shellcodes.org/logo.png']
		>>> htmlCode =  '<img src="http://lx.shellcodes.org/logo.png" />'
		>>> spider.fetchOtherUris(htmlCode)
		['http://lx.shellcodes.org/logo.png']
		>>> htmlCode =  '<IMG src="http://lx.shellcodes.org/logo.png" />'
		>>> spider.fetchOtherUris(htmlCode)
		['http://lx.shellcodes.org/logo.png']
		>>> htmlCode = '<embed src="lx.swf" type="application/x-shockwave-flash"></embed>'
		>>> spider.fetchOtherUris(htmlCode)
		['lx.swf']
		>>> htmlCode = '<embed src="http://lx.com/lx.swf" type="application/x-shockwave-flash"></embed>'
		>>> spider.fetchOtherUris(htmlCode)
		[]
		>>> htmlCode = '<iframe height="256" frameborder="0" width="180" scrolling="" align="center" src="lx.html" name="dmain" marginwidth="0" marginheight="0"></iframe>'
		>>> spider.fetchOtherUris(htmlCode)
		['lx.html']
		"""
		otherUris= []
		scriptTags = re.findall(r"(?i)<script\s.*src\s*=\s*[\"']*([^\"'\s]+).*",htmlCode)
		linkTags = re.findall(r"(?i)<link.*href\s*=\s*[\"']*([^\"'\s]+).*",htmlCode)
		imgTags = re.findall(r"(?i)<img\s.*src\s*=[\"']*([^\"'\s]+).*",htmlCode)
		embedTags = re.findall(r"(?i)<embed\s.*src\s*=[\"']*([^\"'\s]+).*",htmlCode)
		iframeTags = re.findall(r"(?i)<iframe\s.*src\s*=[\"']*([^\"'\s]+).*",htmlCode)

		#不需要来自其他网站的外部链接
		for uri in scriptTags+linkTags+imgTags+embedTags+iframeTags:
			srcURI = urlparse.urlparse(uri)

			#如果链接里有域名，就看是不是来自自己网站的
			if srcURI.netloc == '': otherUris.append(uri)
			elif self.isSameDomain(srcURI.netloc): otherUris.append(uri)
				
		return otherUris

	def fetchUrls(self,htmlCode):
		'''从html代码中提取超链接'''
		return re.findall(r"(?i)<a\s+href\s*=\s*[\"']*([^\"'\s]+)[\"']*[^<]+</a>",htmlCode)

	def saveVisited(self):
		'''将已爬过的连接保存起来'''
		with open(self.domain+'/visited','w') as f:
			cPickle.dump(self.visited,f)

class StoreURI(object):
	def __init__(self,tableName):
		self.uris = []
		self.tableName = tableName.replace('.','_')
		self.db = db.DB(self.tableName)

	def store(self):
		while True:
			try:
				uri = outQueue.get(timeout=3)
				outQueue.task_done()
			except Queue.Empty:
				continue

			if uri == 'kill you,by:lu4nx':break

			if uri not in self.uris:
				self.uris.append(uri)
				print uri
				try: self.storeUrlToDb(uri)
				except Exception,err:
					log.exception('url %s insert to db error:%s'%(uri,err))
					continue

	def storeUrlToDb(self,uri):
		'''实时存储到数据库中'''
		#当前日期
		currentDate = time.strftime('%Y-%m-%d') 
		self.db.insert(self.tableName,uri,currentDate)

def backLogFile(domain):
	'''备份日志文件'''
	if os.path.isfile('%s/%s'%(domain,domain)):
		os.rename('%s/%s'%(domain,domain),'%s/%s'%(domain,domain+'_%s'%time.strftime('%Y%m%d%H%M%S')))

	os.rename(logFile,'%s/%s'%(domain,domain))

def checkHasErrorUrlFromLog(domain,retryFile):
	'''检查是否有爬取失败的url
	>>> checkHasErrorUrl()
	True
	'''
	ret = False
	retryLogFile = '%s/%s'%(domain,retryFile)

	if os.path.isfile(retryLogFile):
		with open(retryLogFile) as logFileObj:
			for line in logFileObj:
				line = line.split('|')

				#只有第一个字符==u时，才对比整个字符串，提高性能
				if line[0][0] == 'u' and line[0] == 'urlerror':
					ret = True
					break

	return ret

def loadErrorUrl(domain,retryFile):
	'''加载所有失败的url'''
	urlList = []
	retryLogFile = '%s/%s'%(domain,retryFile)

	if os.path.isfile(retryLogFile):
		with open(retryLogFile,'r') as errorUrl:
			for line in errorUrl:
				line = line.split('|')
				#不加载404的url
				if line[5].find('404') != -1:continue
				#日志中第5列是url
				else:urlList.append(line[4])

	return urlList

def main(rootUrl,threadCount,retryFile):
	spider = Spider()
	spider.domain = spider.getUrlDomain(rootUrl)
	urlObj = StoreURI(spider.domain)

	#建立存储网站信息的目录
	if not os.path.isdir(spider.domain):os.mkdir(spider.domain)

	#重新爬失败的url
	if retryFile and checkHasErrorUrlFromLog(spider.domain,retryFile):
		#返回需要重新爬取的url列表
		reTryUrls = loadErrorUrl(spider.domain,retryFile)

		if len(reTryUrls) > 0:
			print u'发现有以前失败过的url'
			#载入已爬过的uri
			for uri in urlObj.db.fetchAllData(spider.domain):urlObj.uris.append(uri)

			#载入已爬过的url
			#NOTE:注意这里有个漏洞
			spider.visited = cPickle.load(open(spider.domain+'/visited','r'))

			#载入需要重新爬的url
			for url in reTryUrls:
				urlQueue.put(url)	
		else:urlQueue.put(rootUrl)	
	else:
		#插入根url到队列
		urlQueue.put(rootUrl)

	#创建爬虫的线程池
	for i in range(threadCount):
		thread = threading.Thread(target=spider.spider)
		thread.daemon = True
		threadsPool.append(thread)
		thread.start()
	
	#创建存储uri的线程
	urlThread = threading.Thread(target=urlObj.store)
	urlThread.daemon = True
	urlThread.start()
	
	#等待爬虫线程的结束
	for _ in threadsPool: _.join()

	#保存已爬过的url
	spider.saveVisited()

	#通知存储线程结束
	outQueue.put('kill you,by:lu4nx')

	#等待uri存储线程结束
	urlThread.join()
	print u'工作完毕'

	#备份日志文件
	backLogFile(spider.domain)

if __name__ == '__main__':
	opt = optparse.OptionParser(version='0.1',
				    usage='spider.py -u http://xxx --thread [thread count]', description='my spider,by:lu4nx')

	opt.add_option('-u',help='url',dest='rootUrl')
	opt.add_option('--thread',help=u'线程数，默认10',dest='threadCount',type='int')
	opt.add_option('--retryfile',help=u'指定要重爬的日志文件',dest='retryFile')
	opt.set_defaults(threadCount=10,retryFile=None)

	(opt,args) = opt.parse_args()

#	try:
#		main(opt.rootUrl,opt.threadCount,retryFile)
#	#except Exception,err: print u'参数错误，请加参数-h获得帮助信息';print err
#	except Exception,err:print err
	main(opt.rootUrl,opt.threadCount,opt.retryFile)

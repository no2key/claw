#!/bin/env python
#coding=utf-8

##提取日志用的，跟爬虫无关

import sys,urllib2

uriList = []

if __name__ == '__main__':
	try:
		uriFile = sys.argv[1]
	except:
		print 'usage:cmp [uri file]'
		sys.exit(1)

	with open(uriFile,'r') as uriFileObj:
		for line in uriFileObj:
			uriList.append(line.strip())

	for line in sys.stdin:
		logLine = line
		try:
			line = line.split(' ')[8]
		except:continue

		if urllib2.unquote(line) not in uriList:
			print logLine


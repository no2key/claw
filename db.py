#!coding=utf-8

import sys
try:
	import MySQLdb
except ImportError:
	print 'please:sudo apt-get install python-mysqldb'
	sys.exit(1)

class DB(object):
	def __init__(self,tableName):
		self.conn = None
		self.cur = None
		self.tableName = tableName
		self.connectDB('localhost','root','mysql','claw')

	def connectDB(self,host,userName,passWd,dbName,port=3306):
		try:
			self.conn = MySQLdb.connect(host=host,user=userName,passwd=passWd,db=dbName,port=port)
			self.cur = self.conn.cursor()
			self.conn.select_db(dbName)
			self.cur.execute("create table if not exists %s(id int primary key auto_increment,uri text,update_date varchar(20))"%self.tableName)

#			if self.conn:
#				self.cur.execute("create database if not exists %s"%dbName)
		except MySQLdb.Error,err:
			print err
			sys.exit(1)
			
	def fetchUrls(self,tableName):
		if self.conn:
			self.cur.execute("select * from %s",tableName)
			return self.cur.fetchall()
		else: print 'database connect error'

	def insert(self,tableName,uri,update_date):
		if self.conn:
			#处理掉"."和"-"，不然会出事的
			tableName = tableName.replace('.','_')
			tableName = tableName.replace('-','_')

			#处理掉"\"这样字符的uri，不然insert会报异常
			uri = uri.replace(chr(92),chr(92)+chr(92))

			self.cur.execute("insert into %s(uri,update_date) values ('%s','%s');"%(tableName,uri,update_date))
			self.conn.commit()
		else: print 'database connect error'

	def fetchAllData(self,tableName):
		'''从数据库中返回所有uri'''
		if self.conn:
			tableName = tableName.replace('.','_')
			tableName = tableName.replace('-','_')

			dataCount = self.cur.execute('select uri from %s',tableName)

			if dataCount > 0:
				return self.cur.fetchall()
			else:
				return []
		else: print 'database connect error'
		
	def __del__(self):
		self.cur.close()
		self.conn.close()

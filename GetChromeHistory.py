
import sqlite3 as sql
import os
import pandas as pd
import logging
import argparse
from datetime import datetime, timedelta
import timeit
import time

DIR_PATH = 'C:\Users\User\AppData\Local\Google\Chrome\User Data\Default'
TABLES = []

def get_connection(db='History'):
	conn = sql.connect(os.path.join(DIR_PATH, db))
	conn.text_factory = str
	return conn

def get_tables(conn):
	c = conn.cursor()
	query = 'SELECT * from sqlite_master where type="table"'
	result = c.execute(query)
	logging.info('Fetching table names from database')
	for row in result:
		TABLES.append(str(row[1]))
	c.close()

def get_date(visit_time):
	d = datetime(1601, 1, 1) + timedelta(microseconds=int(visit_time))
	return d

def get_time(visit_date):
	return time.mktime(datetime.strptime(visit_date, "%d/%m/%Y").timetuple())

def get_query_string(args):
	q_params = ''
	a_params = ()
	if len(args.address and args.title) > 0:
		q_params = "url LIKE ? and title LIKE ?"
		a_params = ('%'+args.address+'%', '%'+args.title+'%')
	elif len(args.address or args.title) > 0:
		if len(args.address) > 0:
			q_params = "url LIKE ?"
			a_params = ('%'+args.address+'%',)
		else:
			q_params = "title LIKE ?"
			a_params = ('%'+args.title+'%',)

	if len(q_params) > 1:
		return ('SELECT * from {} where {} limit {}'.format(args.table,
															q_params,
															args.limit), a_params)
	else:
		return ('SELECT * from {} limit {}'.format(args.table, args.limit), a_params)

def get_contents(conn, args):
	c = conn.cursor()
	query, a_params = get_query_string(args)
	result = c.execute(query, a_params)
	df = pd.DataFrame(row for row in result)
	df.columns = list(map(lambda x: x[0], result.description))
	df = df[['url', 'title', 'last_visit_time', 'visit_count']]
	df['last_visit_date'] = (map(lambda visit_time: get_date(visit_time), 
								[visit_time for visit_time in df['last_visit_time']]))

	#df['last_visit_timestamp'] = (map(lambda visit_date: get_time(visit_date),
	#							[visit_date for visit_date in df['last_visit_date']]))

	df = df.sort_values(by='last_visit_date')
	print df

	c.close()

if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	parser = argparse.ArgumentParser()
	parser.add_argument(
		'-table',
        '--table',
        help='Table name from which data needs to be fetched.',
        default='urls')
	parser.add_argument(
		'-sd',
        '--startdate',
        help='From date where to fetch history',
        default=None)
	parser.add_argument(
		'-ed',
        '--enddate',
        help='End date to fetch history',
        default=datetime.now())
	parser.add_argument(
    	'-a',
        '--address',
        help='Specific domain names to search for',
        default='')
	parser.add_argument(
    	'-t',
        '--title',
        help='Specific title of web page to search for',
        default='')
	parser.add_argument(
    	'-l',
        '--limit',
        help='Limits of result',
        default=20)
	args = parser.parse_args()
	logging.info('Establishing connection with the database')
	conn = get_connection()
	logging.info('Table {}'.format(args.table))
	get_contents(conn, args)

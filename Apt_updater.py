import requests
import gzip
from bs4 import BeautifulSoup
import time
import psycopg2
import psycopg2.extras
import pickle
import os
import logging

def getFile(url):
	'''Takes a string containing a url and returns the local path to the file.
	Downloads the file to temp/ folder'''
	response = requests.get(url)
	filename = 'temp/' + url[-(url[::-1].find('/')):]
	
	with open(filename , 'wb') as f:
		f.write(response.content)
		
	return filename

		
def decompress(filename):
	'''Takes a string containing the path to a gzip encoded file.
	Returns the path to the decoded file.
	Files are decoded to same directory as source.'''
	if (filename[-3:] != '.gz'):
		print('This is not a gz file.')

	with gzip.open(filename, 'rb') as f:
		data = f.read()
	
	with open(filename[:-3], 'wb') as f:
		f.write(data)
	
	return filename[:-3] # This is the filename without the .gz
		

def getSoup(url):
	'''Takes the url of a gzip encoded xml file.
	Returns the BeautifulSoup opbject for the file.
	Downloads and decodes the file to temp/ folder.'''
	with open(decompress(getFile(url)), 'rb') as f:
		file = f.read()
	
	return BeautifulSoup(file, 'xml')
	
def getUrls(url):
	'''Recursively crawls a sitemap for urls.
	Returns a Set object containing the urls.'''
	soup = getSoup(url)
	results = set()
	
	if soup.contents[0].name == 'sitemapindex':
		for sitemap in soup.find_all('sitemap'):
			results = results | getUrls(sitemap.find('loc').text)
	elif soup.contents[0].name == 'urlset':
		for url in soup.find_all('url'):
			results.add(url.find('loc').text)
	else:
		print('Error crawling the sitemap.')
		print(url)
		print(soup)
	
	return results	# Make sure to typecast to a list after calling this function if you need to

def crawl_apartments():
	'''Returns a set containing all the links under the apartments.com robot.txt.'''
	print("Scraping urls from the apartments.com robots.txt.")
	
	if not os.path.exists('temp'):
		os.makedirs('temp')
	
	logging.info("Scraping urls from the apartments.com robots.txt.")
	urls = getUrls('https://www.apartments.com/sitemap_AllProfiles.xml.gz')
	print("Done scraping.")
	logging.info("Done scraping.")
	
	return urls
	
def login_to_database():
	'''Wrapper for connect_postgresql() that uses credentials stored in "credentials.py"'''
	import credentials
	try:
		conn, cur = connect_postgresql(host=credentials.host, user=credentials.user, password=credentials.password)
	except:
		print('Retrying connection...')
		time.sleep(1)
		conn, cur = connect_postgresql(host=credentials.host, user=credentials.user, password=credentials.password)
	return conn, cur

def connect_postgresql(
                       host='',
                       user='',
                       password=''):
    """Set up the connection to postgresql database."""
    try:
        conn = psycopg2.connect(
                "dbname ='postgres' host={} user={} \
                 password={}".format(host,user,password))
        cur = conn.cursor()
        return conn,cur
    except Exception as e:
        print("Unable to connect to the database Error is ",e)

def makeTable(cur, table_name = 'apt_active_ids'):
	'''Makes a new table for the crawled apt robots.txt sitemap data.'''
	query = """CREATE TABLE """ + table_name + """ (
	url TEXT UNIQUE NOT NULL);"""
	cur.execute(query)

def dropTable(cur, table_name = 'apt_active_ids'):
	'''Drops the old crawled data table.'''
	query = "DROP TABLE " + table_name
	cur.execute(query)

def insert_into_table(active_urls, batch_size=200):
	'''Inserts the ids into the database 100,000 at a time.'''
	active_temp=active_urls
	active_urls= [[i] for i in active_temp]
	query = '''INSERT INTO apt_active_ids VALUES (%s) '''
	conn, cur = login_to_database()
	while len(active_urls) > 100000:
		print(str(len(active_urls))+ ' urls left to insert.')
		psycopg2.extras.execute_batch(cur, query, active_urls[:100000], page_size=batch_size)
		active_urls = active_urls[100000:]
		conn.commit()
	psycopg2.extras.execute_batch(cur, query, active_urls, page_size=batch_size)
	conn.commit()
	cur.close()
	conn.close()

def save(url_set):
	'''Saves a set of (url, id, date) to a .csv file using pandas.'''
	filename = "Apt_active_ids_" + time.strftime("%d_%m_%Y") + ".csv"
	print("Saving file to: " + filename)
	
	import pandas as pd
	apt_dataframe = pd.DataFrame(list(url_set))
	apt_dataframe.to_csv(filename,index=False)
	
	print('Done saving file.')
	return filename

def main():
	
	# table_name is here just for record keeping
	#table_name = 'bnb_active_ids'
	
	# Recursively crawl the sitemap for url data
	global active_urls
	active_urls = list(crawl_apartments())
	
	# Save the crawled data to a local, dated, .csv file.
	filename = save(active_urls)
	#
	## Open a new connection to database
	conn, cur = login_to_database()
	#
	## Drop the previous table to make room for the updated table
	dropTable(cur)
	conn.commit()
	#
	## Make a new table with all the updated data.
	makeTable(cur)
	conn.commit()
	#
	## Close the connections (insert_into_table makes its own connection)
	cur.close()
	conn.close()
	#
	## Insert all the data into the table
	insert_into_table(active_urls)

if __name__ == "__main__":
	main()
	pass

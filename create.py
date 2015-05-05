#!/usr/bin/env python

####################################
# 
# Module to create a large json file with lists of
# restricted-content sites
#
# To run this:
#  python create.py

####################################
#Types of restricted sites
# - Adult / Pornographic
# - Weapons
# - Drugs
# - Gambling
# - Gore/Violence
# - Alcohol
# - Cult (e.g. Scientology)
# - Terrorism recruitment (e.g. AlQaeda)

####################################
# Sources:
# - Alexa content ratings
# - DMOZ categories
# - JTerry Content Verification List
# - DomainAnalysis
# - TLDs (.xxx)
# - UNT List
# - Domain name matching

from json import dumps

from pymongo import MongoClient
from tldextract import extract

#Accessing particular data sources

def category_chunk(c, chunks):
	"""Searches for domains by matching specific chunks in their
	DMOZ categories.
	Accepts a Connection (c) and an iterable (chunks)
	Returns """
	
	chunks = set(chunks)
	domains = []
	query = {'alexa.DMOZ.SITE.CATS.CAT':{'$exists':True}}
	requirement = {'domain':1, 'alexa.DMOZ.SITE.CATS.CAT':1}
	
	for domain in c['domains'].find(query, requirement):
		try:
			cat_container = domain['alexa']['DMOZ']['SITE']['CATS']['CAT'] #urgh this API
			if cat_container != {}:
				if type(cat_container) == list:
					cats = [x['@ID'] for x in cat_container] #data consistency, anyone?
				else:
					cats = [cat_container['@ID']]
				
				for cat in cats:
					cat = cat.split('/')
					for chunk in cat:
						if chunk in chunks:
							domain_name = domain['domain'].replace('#', '.')
							domains.append(domain_name)
							break
		except KeyError:
			continue
	
	return domains

def check_domain_analysis(category):
	"""Domain Analysis is a large spreadsheet with several classifications on it"""
	domains = []
	
	with open('domain_analysis.tsv') as f:
		for line in f:
			line = line.split('\t')
			domain = line[0]
			categories = line[1]
			if category in categories:
				domains.append(domain)
	
	return domains

#Checkers

def check_toulouse_list():
	"""A university in Toulouse provides a gigantic blacklist: http://dsi.ut-capitole.fr/blacklists/index_en.php.
	This checks the latest alexa top 1m against it. Requires two files (see first few lines)
	
	These are in toulouse_adult.dump. The problem is that many popular sites are false positives e.g. yahoo.com.
	"""
	
	adult_payload_location = "/Users/mruttley/Documents/2015-05-01 Blacklist/contentfilter/adult/"
	top_1m_location = "/Users/mruttley/Documents/2015-04-22 AdGroups/Bucketerer/data_crunching/ranking_files/2015-05-04top-1m.csv"
	
	domains = set()
	for fn in ['domains']:
		with open(adult_payload_location + fn) as f:
			print "importing adult payload"
			for n, line in enumerate(f):
				if len(line) > 4: #some weird line ending stuff
					domain_info = extract(line[:-1])
					domain_name = domain_info.domain + "." + domain_info.suffix
					domains.update([domain_name])
				if n % 10000 == 0:
					print n
	
	print "Checking 1m sites"
	exists = 0
	with open(top_1m_location) as f:
		with open('toulouse_adult.dump', 'w') as g:
			for n, line in enumerate(f):
				if len(line) > 4:
					domain = line.split(',')[1][:-1]
					if domain in domains:
						exists += 1
						g.write(domain + "\n")
				if n % 10000 == 0:
					print n, exists
	print exists

def check_comscore_list():
	"""Comscore provides about 2500 top adult sites. Which are in the Alexa top 1m?
	"""
	
	adult_payload_location = "/Users/mruttley/Documents/2015-05-01 Blacklist/contentfilter/"
	top_1m_location = "/Users/mruttley/Documents/2015-04-22 AdGroups/Bucketerer/data_crunching/ranking_files/2015-05-05top-1m.csv"
	
	domains = set()
	for fn in ['comscore_adult_sites.txt']:
		with open(adult_payload_location + fn) as f:
			print "importing adult payload"
			for n, line in enumerate(f):
				if len(line) > 4: #some weird line ending stuff
					if line.endswith('\n'):
						line = line[:-1]
					domains.update([line])
				if n % 10000 == 0:
					print n
	
	print "Checking 1m sites"
	exists = 0
	with open(top_1m_location) as f:
		with open('comscore_adult_top1m.dump', 'w') as g:
			for n, line in enumerate(f):
				if len(line) > 4:
					domain = line.split(',')[1][:-1]
					if domain in domains:
						exists += 1
						g.write(domain + "\n")
				if n % 10000 == 0:
					print n, exists
	print exists

#Handlers for each genre

def get_adult_sites():
	"""Gets adult sites from various data sources"""
	
	stats = {
		'bucketerer_cats': 0,
		'content verification': 0,
		'domain analysis': 0,
		'tld': 0,
		'unt_list': 0,
		'domain_name_matching': 0
	}

	domains = set()
	
	#Get sites from bucketerer db
	db_sites = category_chunk(c, ["Adult"])
	domains.update(db_sites)
	stats['bucketerer_cats'] += len(db_sites)
	
	#get sites from DomainAnalysis
	domain_analysis = check_domain_analysis('18')
	domains.update(domain_analysis)
	stats['domain analysis'] += len(domain_analysis)
	
	#get sites by tld
	for domain in c['domains'].find({}, {'domain':1}):
		if domain['domain'].endswith('xxx'):
			domains.update([domain['domain'].replace('#', '.')])
			stats['tld'] += 1
	
	#get comscore sites
	with open('comscore_adult_top1m.dump') as f:
		for line in f:
			if len(line) > 4:
				if line.endswith('\n'):
					line = line[:-1]
				domains.update([line])
	
	for k,v in stats.iteritems():
		print k,v
	
	return sorted(list(domains))

def get_gambling_sites():
	"""Gets gambling sites"""
	
	domains = set()
	
	#get domains from the bucketerer database
	matchers = [
		'Poker', 'Gambling', 'Blackjack'
	]
	dbdomains = category_chunk(c, matchers)
	domains.update(dbdomains)
	
	return sorted(list(domains))

def get_drugs_sites():
	"""Gets drugs sites"""
	
	domains = set()
	
	#get domains from the bucketerer database
	matchers = [
		"Drugs"
	]
	dbdomains = category_chunk(c, matchers)
	domains.update(dbdomains)
	
	with open("other_drugs_sites.txt") as f:
		for line in f:
			if len(line) > 4:
				domains.update([line[:-1]])
	
	return sorted(list(domains))

def get_alcohol_sites():
	"""Gets alcohol related sites"""
	
	domains = set()
	
	#get domains from the bucketerer database
	matchers = [
		"Wine", "Beer", "Liquor"
	]
	dbdomains = category_chunk(c, matchers)
	domains.update(dbdomains)
	
	return sorted(list(domains))	

#Main Handler
if __name__ == "__main__":
	#Set up database connection
	c = MongoClient()['bucketerer']
	
	#container
	sites = {}
	
	#get sites from each genre we're concerned about
	sites['adult'] = get_adult_sites()
	sites['gambling'] = get_gambling_sites()
	sites['drugs'] = get_drugs_sites()
	sites['alcohol'] = get_alcohol_sites()
	
	#dump to json file
	with open('sites.json', 'w') as f:
		sites= dumps(sites, indent=4)
		f.write(sites)






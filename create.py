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





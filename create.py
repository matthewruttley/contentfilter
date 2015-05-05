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
from datetime import datetime
from os import listdir

from pymongo import MongoClient
from tldextract import extract

#Accessing particular data sources

def category_chunk(c, chunks, negative=False):
	"""Searches for domains by matching specific chunks in their
	DMOZ categories.
	Accepts a Connection (c) and an iterable (chunks)
	Returns """
	
	chunks = set(chunks)
	domains = []
	query = {'alexa.DMOZ.SITE.CATS.CAT':{'$exists':True}}
	requirement = {'domain':1, 'alexa.DMOZ.SITE.CATS.CAT':1}
	
	for domain in c['domains'].find(query, requirement):
		negative_flag = False
		try:
			cat_container = domain['alexa']['DMOZ']['SITE']['CATS']['CAT'] #urgh this API
			if cat_container != {}:
				if type(cat_container) == list:
					cats = [x['@ID'] for x in cat_container] #data consistency, anyone?
				else:
					cats = [cat_container['@ID']]
				
				if negative:
					for cat in cats: #pretty inefficient but gets the job done
						cat = set(cat.split('/'))							
						if negative.intersection(cat):
							negative_flag = True
				
				if not negative_flag:
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
	"""Domain Analysis is a large spreadsheet with about 1000 several hand classified domains"""
	domains = []
	
	with open('sources/hand_classified/domain_analysis.tsv') as f:
		for line in f:
			line = line.split('\t')
			domain = line[0]
			categories = line[1]
			if category in categories:
				domains.append(domain)
	
	return domains

def load_alexa():
	"""Returns a set of all the domains in the latest Alexa top 1m list"""
	timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d')
	top_1m_location = "/Users/mruttley/Documents/2015-04-22 AdGroups/Bucketerer/data_crunching/ranking_files/"+timestamp+"top-1m.csv"
	alexa = set()
	with open(top_1m_location) as f:
		for n, line in enumerate(f):
			if len(line) > 4:
				if line.endswith('\n'):
					line = line[:-1]
				domain = line.lower().split(',')[1]
				alexa.update([domain])
	return alexa

def prepare_comscore_lists():
	"""Cleans and prepares comscore lists. Only returns files that are in the
	latest Alexa top 1m sites"""
	
	#setup
	directory = 'sources/comscore/'
	alexa = load_alexa() #import alexa
	
	#import each list
	for filename in listdir(directory):
		if filename.endswith("dump") == False:
			print "Working on {0}".format(filename)
			domains = set()
			category = filename.split(".")[0] #filenames are in the format: category.txt
			exists = 0
			
			with open(directory + filename) as f:
				for n, line in enumerate(f):
					line = line.lower()
					if len(line) > 4:
						if line.endswith('\n'):
							line = line[:-1]
						if line.endswith("*"):
							line = line[:-1]
						if " " not in line:
							domains.update([line])
						
			print "Checking against Alexa"
			with open(directory + category + '.dump', 'w') as g:
				for domain in domains:
					if domain in alexa:
						exists += 1
						g.write(domain + "\n")
			
			print "Wrote {0} domains to {1}{2}.dump".format(exists, directory, category)

#Checkers

def check_toulouse_list():
	"""A university in Toulouse provides a gigantic blacklist: http://dsi.ut-capitole.fr/blacklists/index_en.php.
	This checks the latest alexa top 1m against it. Requires two files (see first few lines)
	"""
	
	payload_directory = "sources/toulouse/adult/"
	payload_fn = "domain"
	
	domains = set()
	alexa = load_alexa()
	exists = 0
	
	with open(payload_directory + payload_fn) as f:
		with open('toulouse_check.dump', 'w') as g:
			print "Importing Toulouse payload"
			for n, line in enumerate(f):
				if len(line) > 4: #some weird line ending stuff
					if line.endswith('\n'):
						line = line[:-1]
					domain_info = extract(line)
					if domain_info.subdomain == "":
						domain_name = domain_info.domain + "." + domain_info.suffix
						if domain_name in alexa:
							g.write(domain_name + "\n")
							exists += 1
	
	print "{0} found in Alexa. Written to toulouse_check.dump".format()

#Handlers for each genre

def get_adult_sites():
	"""Gets adult sites from various data sources"""

	domains = set()
	
	#Get sites from bucketerer db
	db_sites = category_chunk(c, ["Adult"])
	domains.update(db_sites)
	
	#get sites from DomainAnalysis
	domain_analysis = check_domain_analysis('18')
	domains.update(domain_analysis)
	
	#get sites by tld
	for domain in c['domains'].find({}, {'domain':1}):
		if domain['domain'].endswith('xxx'):
			domains.update([domain['domain'].replace('#', '.')])
	
	#get comscore sites
	with open('sources/comscore/adult.dump') as f:
		for line in f:
			if len(line) > 4:
				if line.endswith('\n'):
					line = line[:-1]
				domains.update([line])
	
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
	
	with open("sources/suggested/drugs.txt") as f:
		for line in f:
			if len(line) > 4:
				if line.endswith('\n'):
					line = line[:-1]
				domains.update([line])
	
	return sorted(list(domains))

def get_alcohol_sites():
	"""Gets alcohol related sites"""
	
	domains = set()
	
	#get domains from the bucketerer database
	matchers = [
		"Wine", "Beer", "Liquor"
	]
	
	
	negative = ["DOS_and_Windows"]
	negative = set([unicode(x) for x in negative])
	
	dbdomains = category_chunk(c, matchers, negative=negative)
	domains.update(dbdomains)
	
	return sorted(list(domains))	

#Main Handler
if __name__ == "__main__":
	#Set up database connection
	c = MongoClient()['bucketerer']
	
	#container
	sites = {}
	
	#prepare comscore stuff
	prepare_comscore_lists()
	
	#get sites from each genre we're concerned about
	print "Processing Adult Sites"; sites['adult'] = get_adult_sites()
	print "Processing Gambling Sites"; sites['gambling'] = get_gambling_sites()
	print "Processing Drugs Sites"; sites['drugs'] = get_drugs_sites()
	print "Processing Alcohol Sites"; sites['alcohol'] = get_alcohol_sites()
	
	#dump to json file
	with open('sites.json', 'w') as f:
		sites= dumps(sites, indent=4)
		f.write(sites)

from bs4 import BeautifulSoup as BS, Comment
import requests
import traceback
import urllib.request
import os.path
import csv
import threading
import logging
import pprint
import math
import re

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('(%(threadName)-10s) - %(asctime)s - %(levelname)s - %(message)s')

fh = logging.FileHandler('logfile.txt')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

email_regex = re.compile(("([a-z0-9!#$%&'*+\/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+\/=?^_`"
                    "{|}~-]+)*(@|\sat\s)(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(\.|"
                    "\sdot\s))+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)"))

request_timeout = 5;

class ParserManager:
	def __init__(self, urls_file):
		domains_file = open(urls_file, 'r');
		websites_str = domains_file.read();
		websites_list = websites_str.replace(" ","").split(",");
		domains_file.close();

		threadsAmount = 10;
		threads = [];

		for i in range(threadsAmount):
			url_partial_list = self.get_list_part(websites_list, threadsAmount, i);
			t = threading.Thread(target=BunchParser, args=(url_partial_list,))
			threads.append(t)
			t.start()
		
	def get_list_part(self, urls_list, total_parts, current_part_number):
		n = math.ceil(len(urls_list)/total_parts);
		return urls_list[current_part_number*n:n*(current_part_number+1)];


class BunchParser:
	def __init__(self, urls_list):
		parsers = [];
		counter = 0;

		for url in urls_list:
			counter += 1;
			url = "http://www." + url;
			parsers.append(Parser(url));
			logger.debug("Parsed: " + str(round((counter/len(urls_list))*100, 2)) + "%" + " of websites");
			#logger.debug("-----" + url + "-----------------------");

		return

class WebsiteModel:

	def __init__(self):
		self.swf_list = [];
		self.emails = [];
		self.base_url = "";

	def doesSWFExists(self):
		return self.swf_list;

	def addEmails(self, new_emails):
		for new_email in new_emails:
			for email in self.emails:
				if email == new_email:
					return;
			
			self.emails.append(new_email);
					

	def addSwfLocation(self, url):
		for swf_vo in self.swf_list:
			if swf_vo.swf_location == url:
				return;

		new_swfvo = SwfVO();	
		new_swfvo.swf_location = url;	
		#logger.debug("Adding to SWF LIST: " + url);
		self.swf_list.append(new_swfvo);	

		return new_swfvo;


class SwfVO:

	def __init__(self):
		self.swf_path = "";
		self.swf_location = "";
		self.swf_size = 0;


class Parser:
	def __init__(self, url):
		self.parsed_links = set()
		self.links_to_be_parsed = set()
		self.csv_file_name = "results.csv"
		self.max_file_size = 1000000;
		self.max_links_in_one_domain = 100;

		self.website_model = WebsiteModel();
		self.website_model.base_url = url;

		self.base_url = url
		self.links_to_be_parsed.add(url)
		self.parse_site()
		

		#http[^\s]+(.swf)
		#[^\s"']+(.swf)

	def get_emails(self, s):
		return [email[0] for email in re.findall(email_regex, s) if not email[0].startswith('//')];

	def parse_url(self, url):
		""" Parses single link of a site """
		# logger.debug("LTBP", self.links_to_be_parsed)
		self.parsed_links.add(url)
		try:
			r = requests.get(url, timeout=request_timeout);
			#Let's not parse files with more than 1MB in size
			logger.debug(r);
			if(len(r.content) < self.max_file_size):
				html = r.text
				soup = BS(html, "html.parser")
				comments = soup.findAll(text=lambda text:isinstance(text, Comment))
				for comment in comments:
					comment.extract()
				new_emails = self.get_emails(html);

				if new_emails:
					self.website_model.addEmails(new_emails);

				swf_path = "";
				if '.swf' in html:
					searchSWfObj = re.search("http[^\s]+(\.swf)", html)
					searchSWfRelative = re.search("[^\s\"']+(\.swf)", html)
					swf_vo = self.website_model.addSwfLocation(url);
					if searchSWfObj:
						swf_path = str(searchSWfObj.group());
					elif searchSWfRelative:
						swf_path = str(searchSWfRelative.group());
					# For the situation where \/ occured in path
					swf_path = swf_path.replace("\\", "")

				if swf_path:		
					#logger.debug("Prepare to download SWF!")
					#Workaround for relative path
					if "http" not in swf_path:
						if swf_path.startswith("/"):
							swf_vo.swf_path = swf_vo.swf_location + swf_path;					
						else:
							swf_vo.swf_path = swf_vo.swf_location + "/" + swf_path;
					else:
						swf_vo.swf_path = swf_path;
					print ("!!!!!!!!! SWF path", swf_path)
					#logger.debug("swf_path: " + swf_path);
					self.download_swf(swf_vo)


				all_links = set(soup.find_all('a'))
				for link in all_links:
					link_href = link.get("href")
					full_link = self.build_url(link_href)

					if full_link:
						if full_link not in self.links_to_be_parsed and full_link not in self.parsed_links:
							self.links_to_be_parsed.add(full_link)

		except Exception as ex:
			template = "An exception of type {0} occured. Arguments:\n{1!r}"
			message = template.format(type(ex).__name__, ex.args)
			logger.error(message) 	

	def build_url(self, link):
		if link is None:
			return
		elif link == "#" or link == "/":
			return
		elif link.endswith(".jpg") or link.endswith(".pdf") or link.endswith(".png") or link.endswith(".jpeg") or link.endswith(".mp3"):
		    return
		elif link.endswith(".mov") or link.endswith(".avi") or link.endswith(".mp4") or link.endswith(".flv") or link.endswith(".doc") or link.endswith(".xls"):
		    return    
		elif link.startswith("#") or link.startswith("?"):
			return	
		elif "mailto" in link:
			return
		elif link.startswith("http") and not link.startswith(self.base_url):
			return
		elif link.startswith("/"): 
			return self.base_url + link
		elif not link.startswith("http"):
			return self.base_url + "/" + link
		else:
			return link


	def download_swf(self, swf_vo):
		""" Downloads swf and measures it's size """
		try:
			r = requests.get(swf_vo.swf_path, timeout=request_timeout);
			size = len(r.content) / 1000000 # Convert to Megabytes
			swf_vo.swf_size = size
		except:
			logger.error("SWF DOWNLOADING ERROR {}".format(swf_vo.swf_path))
			traceback.print_exc()

	def write_to_csv(self):
		file_exists = os.path.isfile(self.csv_file_name)
		with open(self.csv_file_name, 'a') as csvfile:
			fieldnames = ('LOCATION', 'URL', 'SIZE', 'EMAILS')
			writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
			if not file_exists:
				writer.writeheader()

			for swf_vo in self.website_model.swf_list:
				writer.writerow({"LOCATION": swf_vo.swf_location, "URL": swf_vo.swf_path, "SIZE": swf_vo.swf_size})

			if self.website_model.swf_list and self.website_model.emails:
				writer.writerow({"EMAILS": str(self.website_model.emails)});

	def parse_site(self):
		while len(self.links_to_be_parsed) > 0 and len(self.parsed_links) < self.max_links_in_one_domain:
			#logger.debug("PARSED: " + str(len(self.parsed_links)) + " TO BE PARSED: " + str(len(self.links_to_be_parsed)))
			link = self.links_to_be_parsed.pop()
			#logger.debug("I'M going to parse " + link)
			self.parse_url(link)

		if self.website_model.doesSWFExists():
			self.write_to_csv()

# p = ParserManager("parse2.txt")
p = Parser("http://www.tv2.no")


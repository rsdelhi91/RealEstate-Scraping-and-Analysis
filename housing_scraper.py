# This script is used to scrape property information from the website - Realestate.com.au, to
# scrape and analyze data for educational purposes only. The Data scraped from this exercise
# will not be made public and is subject to learn practical machine learning and hypothesis
# building exercises only. This script is only used to perform the scraping operation while
# the analysis operations are performed separately as either Map Reduce functions in CouchDB
# or Python code, using Pandas, Scikit, and Matplotlib.
#
# Author Name: Rahul Sharma
# Email: sharma1@student.unimelb.edu.au
#
# This code can be run using Python 3. Initial testing and deployment was done using Anaconda
# Python. It connects and stores data into a CouchDB database deployed on an OpenStack cloud
# instance maintained by me.
#

import requests
import re
import couchdb
import urllib.request
import json
import _thread
from bs4 import BeautifulSoup


## Main Scraping Logic and Establishing Connection to a CouchDB instance

new_lst = []

# A connection to a VM hosting a CouchDB instance is made. Once the connection is established,
# we verify if the CouchDB instance has a database called "housing". If yes, we establish reference
# it and store its access into the variable db, else, we create a new database called "housing".

try:
    couch = couchdb.Server('http://<Enter the IP-Address of remote/local instance running CouchDB>:5984/')
    db = couch['housing']
except:
    db = couch.create('housing')

# This function is used to create a connection with the main page of the website (RealEstate.com.au),
# hosting multiple cells, where each cell corresponds to a single property posting. This function
# scrapes all the links of those postings (cells) and stores them in a list for further scraping, to
# scrape data from each property posting once by one.

def main_page(pg_url):

    # Used for verifying whether the last page in the pagination has been reached.
    global flag
    flag = 0

    # Used to scrape the page whose url is passed to start the process.
    req = requests.get(pg_url)

    # Used to convert the scraped html format into an easy to read format.
    soup = BeautifulSoup(req.content)

    # Used to find the pagination section of the web page and identify the next page link.
    ref_lst = soup.find_all("a", {"class":"detailsButton"})
    next_pg_link = soup.find("a", {"title":"View the next page of results"})

    # Used to scrape the links of all the property postings on the main page and store it in our list.
    for link in ref_lst:
        if link not in new_lst:
            new_lst.append(link.get("href"))

    # If the last page of the particular suburb has been reached, set flag and return execution to start
    # the execution of the next available suburb.
    try:
        new_lst_pg = next_pg_link.get("href")
        global url
        url = "http://www.realestate.com.au" + new_lst_pg
    except:
        flag = 1
        return


# This method is used to scrape all the property cells scraped from the main page. It opens the
# cells web page and scrapes its data like: price, property type, street address, locality, region,
# features, agent info, and so on.

def scrape_link(link, tag):

    # This dictionary will be used to create the json messages to store onto CouchDB.
    data = {}

    # The tiles link will be re-created using the link passed through the main scraped list.
    prop_url = "http://www.realestate.com.au"+link

    # The tiles will be treated as individual pages and opened using urllib. The try will ensure that
    # if a link is no longer working then the next available tile will be processed.
    try:
        prop_req = urllib.request.urlopen(prop_url)
        page_soup = BeautifulSoup(prop_req)
    except:
        return

    # Find the tags matching the class priceText and scrape the price of the property if available
    # else set it to "N/A".
    if not page_soup.find("p", class_="priceText") == None:
        prop_price = page_soup.find("p", class_="priceText").get_text()
    else:
        prop_price = "N/A"

    # Find the tags matching the class streetAddress and scrape the address of the property if available
    # else set it to "N/A".
    if not page_soup.find(itemprop="streetAddress") == None:
        if not page_soup.find(itemprop="streetAddress").get_text() == "":
            prop_street = page_soup.find(itemprop="streetAddress").get_text()
        else:
            prop_street = "N/A"
    else:
        prop_street = "N/A"

    # Find the tags matching the class addressLocality and scrape the locality of the property if available
    # else set it to "N/A".
    if not page_soup.find(itemprop="addressLocality") == None:
        prop_locality = page_soup.find(itemprop="addressLocality").get_text()
    else:
        prop_locality = "N/A"

    # Find the tags matching the class addressRegion and scrape the region of the property if available
    # else set it to "N/A".
    if not page_soup.find(itemprop="addressRegion") == None:
        prop_region = page_soup.find(itemprop="addressRegion").get_text()
    else:
        prop_region = "N/A"

    # Find the tags matching the class postalCode and scrape the postcode of the property if available
    # else set it to "N/A".
    if not page_soup.find(itemprop="postalCode") == None:
        prop_postcode = page_soup.find(itemprop="postalCode").get_text()
    else:
        prop_postcode = "N/A"

    # Find the tags matching the class featureList and scrape the features of the property if available
    # else set all features to "N/A".
    if not page_soup.find("div", class_="featureList") == None:

        # Scrape the features list block on the web page.
        prop_features = page_soup.find("div", class_="featureList").get_text()

        # Set the regex to extract the Property Type from the scraped feature List if available.
        regex = 'Property Type:\w+'
        sub_text = re.search(regex,prop_features)
        if not sub_text == None:
            property_type = sub_text.group(0).replace("Property Type:","")
        else:
            property_type = "N/A"

        # Set the regex to extract the Bedroom from the scraped feature List if available.
        regex = 'Bedrooms:\w+'
        sub_text = re.search(regex,prop_features)
        if not sub_text == None:
            bedrooms = sub_text.group(0).replace("Bedrooms:","")
        else:
            bedrooms = "N/A"

        # Set the regex to extract the Bathrooms from the scraped feature List if available.
        regex = 'Bathrooms:\w+'
        sub_text = re.search(regex,prop_features)
        if not sub_text == None:
            bathrooms = sub_text.group(0).replace("Bathrooms:","")
        else:
            bathrooms = "N/A"

        # Set the regex to extract the Building Size from the scraped feature List if available.
        regex = 'Building Size:(\w)+[.\w ]+'
        sub_text = re.search(regex,prop_features)
        if not sub_text == None:
            building_size = sub_text.group(0).replace("Building Size:","")
        else:
            building_size = "N/A"

        # Set the regex to extract the approx Land Size from the scraped feature List if available.
        regex = '(approx Land Size:(\w)+[.\w ]+)|(Land Size:(\w)+[.\w ]+)'
        sub_text = re.search(regex,prop_features)
        if not sub_text == None:
            approx_land_size = sub_text.group(0).replace("approx Land Size:","").replace("Land Size:","")
        else:
            approx_land_size = "N/A"

        # Set the regex to extract the Indoor Features from the scraped feature List if available.
        regex = 'Indoor Features [\w :-]+'
        sub_text = re.search(regex,prop_features)
        if not sub_text == None:
            indoor_f = sub_text.group(0)
        else:
            indoor_f = "N/A"
    else:
        prop_features = "N/A"
        property_type = "N/A"
        bedrooms = "N/A"
        bathrooms = "N/A"
        building_size = "N/A"
        approx_land_size = "N/A"
        indoor_f = "N/A"

    # This will extract the features for pages which act as advertisements for newly available property.
    if not page_soup.find("div", class_="body-copy") == None:
        prop_features = page_soup.find("div", class_="body-copy").get_text()

    # This will extrat details about the agent dealing with that property. Details like Agent name and
    # contact number is scraped where available.
    if not page_soup.find("div", class_="agentDetailWithEmailAgent") == None:
        agent_address = page_soup.find("div", class_="agentDetailWithEmailAgent").get_text()
        data["agent_address"] = agent_address.replace(" View Agency","").replace(" ProfileEmail","").replace(" AgentView Website","").lstrip()

    # Used to store agent information temporarily before adding it into the JSON message.
    agent_dict = {}
    if not page_soup.find_all("div", class_="agentContactInfo") == None:
        count = 0
        agent_lst = page_soup.find_all("div", class_="agentContactInfo")
        print("************************************")

        # Used to extract the agents name and contact information from the scraped html rendering.
        regexOne = "strong>[A-Za-z ]+"
        regexTwo = "value=\"[\w ]+"

        for agent in agent_lst:
            name = re.search(regexOne,str(agent))
            phone = re.search(regexTwo,str(agent))
            try:
                agent_dict[name.group(0).replace("strong>","")] = phone.group(0).replace("value=\"","").replace(" ","")
            except:
                data["agent_name"+str(count)] = "N/A"

        # Used to add agent information into our Data JSON message (Document to be stored).
        for k,v in agent_dict.items():
            data["agent_name"+str(count)] = k
            data["agent_phone"+str(count)] = v
            count += 1

    # If this property has not been stored into CouchDB, then add the relevant details of this property as a
    # document (JSON Message) into the CouchDB instance as a Key, Value pair, where the key is the property link.
    # Since each property is referenced using a unique url/ link to refer to its post, we set it up as our key
    # as it works as a hashmap where each entry has a unique key. Hence, the _id of the documents being stored
    # in our NoSQL (CouchDB) Database will be the link of the property on Realestate.com.au.
    if link not in db:
        data["_id"] = link
        data["price"] = prop_price
        data["street"] = prop_street
        data["locality"] = prop_locality
        data["region"] = prop_region
        data["postcode"] = prop_postcode
        data["features"] = prop_features
        data["property"] = property_type
        data["bedroom"] = bedrooms
        data["bathroom"] = bathrooms
        data["building_size"] = building_size
        data["approx_land_size"] = approx_land_size
        data["indoor_f"] = indoor_f
        data["tag"] = tag
        doc = json.dumps(data)
        doc_one = json.loads(doc)

        # Save the above created JSON message into CouchDB.
        db.save(doc_one)



## Starting Script

# This process can be made more efficient by setting up the calls as a separate thread with each
# thread managing a range of postcodes, for ex - 3000 to 3200, 3201 to 3400, and so on. The addresses
# we seek are of the form 3XXX with the range being from 3000 to 3996. I am ignoring the postcodes
# in the 8XXX range as they are generally used for Administrative purposes and do not reflect realestate
# containing postcodes.
#
# Initial Url to start the scraping process. This url will be passed to the main_page method to store
# the links of all the cells on that page and then call the scrape_link method to begin scraping individual
# cells/ web pages.
url = "http://www.realestate.com.au/buy/in-3000/list-1?source=location-search"
flag = 0
for page in range(3000,3997):
    for i in range(500):
        print(url)
        main_page(url)
        print("******************")
        print()
        print("******************")
        for tile in new_lst:
            print(tile)
            scrape_link(tile, "buy")
        new_lst.clear()
        if flag == 1:
            break

    url = "http://www.realestate.com.au/buy/in-" + str(page+1) + "/list-1"

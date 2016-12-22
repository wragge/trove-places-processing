import requests
import time
from operator import itemgetter
from pymongo import MongoClient, GEO2D
import re
import string
from nltk import bigrams, word_tokenize
from nltk.corpus import stopwords
import csv

import credentials

STATES = {
    'ACT': 'ACT',
    'New South Wales': 'NSW',
    'Northern Territory': 'NT',
    'South Australia': 'SA',
    'Queensland': 'QLD',
    'Tasmania': 'TAS',
    'Victoria': 'VIC',
    'Western Australia': 'WA'
}


def get_titles():
    '''
    Harvest newspaper details from the Trove API.
    '''
    url = 'http://api.trove.nla.gov.au/newspaper/titles'
    title_url = 'http://api.trove.nla.gov.au/newspaper/title/{}'
    params = {
        'key': credentials.TROVE_API_KEY,
        'encoding': 'json',
        'include': 'years'
    }
    results = get_results(url, params)
    client = MongoClient()
    db = client.trove_places
    collection = db.titles
    for result in results['response']['records']['newspaper']:
        url = title_url.format(result['id'])
        details = get_results(url, params)
        newspaper = details['newspaper']
        newspaper['_id'] = newspaper['id']
        try:
            newspaper['state_id'] = STATES[newspaper['state']]
        except KeyError:
            pass
        collection.save(newspaper)
        time.sleep(1)


def check_if_place(word):
    client = MongoClient()
    db = client.trove_places
    places = db.places
    place = places.find_one({'name_lower': word})
    return place


def get_title_words():
    '''
    Extract a list of words from titles ordered by frequency.
    I originally thought I needed to generate a list of stopwords using this, but it turned out it wasn't necessary.
    '''
    words = {}
    client = MongoClient()
    db = client.trove_places
    titles = db.titles
    for newspaper in titles.find():
        title = newspaper['title']
        title = re.sub(r'\(.*\)', '', title).strip()
        stop = stopwords.words('english') + [p for p in string.punctuation]
        title_words = [word for word in word_tokenize(title.lower()) if word not in stop]
        for word in title_words:
            try:
                words[word] += 1
            except KeyError:
                words[word] = 1
    words = sorted(words.items(), key=itemgetter(1), reverse=True)
    with open('data/title_words_totals.txt', 'wb') as titles_file:
        for word in words:
            # if word[1] > 1:
            print '{} - {}'.format(word[0].encode('utf-8'), word[1])
            # titles_file.write('{}\n'.format(word[0].encode('utf-8').lower()))
            titles_file.write('{} - {}\n'.format(word[0].encode('utf-8'), word[1]))
    with open('data/title_words.txt', 'wb') as titles_file:
        for word in words:
            if not check_if_place(word[0]):
                titles_file.write('{}\n'.format(word[0].encode('utf-8').lower()))


def locate_titles():
    '''
    Tokenize titles and look-up each uni/bigram in the places db.
    Write the results to a CSV file for manual checking/editing.
    '''
    not_found = 0
    client = MongoClient()
    db = client.trove_places
    titles = db.titles
    places = db.places
    title_stopwords = []
    # Add any words you don't want to treated as potential places
    with open('data/title_stop_words.txt', 'rb') as title_stop_file:
        for title_word in iter(title_stop_file):
            title_stopwords.append(title_word.lower().strip())
    # We're going to write results to a CSV file for checking
    with open('data/titles.csv', 'wb') as titles_csv:
        writer = csv.writer(titles_csv)
        for newspaper in titles.find().sort('name'):
            newspaper['places'] = []
            try:
                state = newspaper['state_id']
            except KeyError:
                pass
            else:
                title = newspaper['title']
                if 'state_id' in newspaper:
                    state_id = newspaper['state_id']
                else:
                    state_id = newspaper['state']
                try:
                    # Get things that could be placenames in the brackets at the end of titles
                    placename = re.search(r'\(([A-Za-z \/]+),', title).group(1)
                    # print placename
                except AttributeError:
                    placename = None
                # print '\n{}'.format(title.encode('utf-8'))
                # Remove the stuff in brackets from the title
                title = re.sub(r'\(.*\)', '', title).strip().encode('utf-8')
                # Add the de-bracketed names back to the title
                if placename:
                    title = '{} {}'.format(title, placename)
                stop = stopwords.words('english') + [p for p in string.punctuation] + title_stopwords
                # Tokenize titles -- unigrams and bigrams
                title_words = [word for word in word_tokenize(title.replace('-', ' ').replace('/', ' ').lower()) if word not in stop]
                title_bigrams = bigrams(title_words)
                # Check the bigrams first
                for title_bigram in title_bigrams:
                    # Look up each bigram in the places db
                    place = places.find_one({'name_lower': ' '.join(title_bigram), 'state': state, 'display': 'y'})
                    if place:
                        # print '  {}'.format(place['name'])
                        writer.writerow([newspaper['_id'], newspaper['title'].encode('utf-8'), state_id, place['_id'], place['name'].title(), place['loc'][1], place['loc'][0]])
                        newspaper['places'].append(place)
                # Now check for unigrams
                for title_word in title_words:
                    place = places.find_one({'name_lower': title_word.lower(), 'state': state, 'display': 'y'})
                    if place and place not in newspaper['places']:
                        # print '  {}'.format(place['name'])
                        writer.writerow([newspaper['_id'], newspaper['title'].encode('utf-8'), state_id, place['_id'], place['name'].title(), place['loc'][1], place['loc'][0]])
                        newspaper['places'].append(place)
            # Write titles without places to the CSV file for manual checking
            if not newspaper['places']:
                writer.writerow([newspaper['_id'], newspaper['title'].encode('utf-8'), state_id, '', '', '', ''])
                not_found += 1
            # titles.save(newspaper)
    print not_found


def load_places():
    '''
    Once the CSV file has been manually checked, use this to load the place data into the title records.
    '''
    client = MongoClient()
    db = client.trove_places
    titles = db.titles
    places = db.places
    with open('data/trove-newspaper-titles-locations.csv', 'rb') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            if row[3]:
                place = places.find_one({'_id': row[3]})
                print place['name']
                titles.update_one({'_id': row[0]}, {'$push': {'places': place}})


def find_titles(placename, state):
    placename = placename.lower()
    client = MongoClient()
    db = client.trove_places
    titles = db.titles
    places = db.places
    titles.ensure_index([('places.loc', GEO2D)], min=-500, max=500)
    place = places.find_one({'name_lower': placename, 'state': state})
    if place:
        loc = place['loc']
        near_titles = titles.find({"places.loc": {"$near": loc}}).limit(10)
        for title in near_titles:
            print title['title']


def get_results(url, params):
    '''
    Get json data.
    '''
    r = requests.get(url, params=params)
    print r.url
    results = r.json()
    return results

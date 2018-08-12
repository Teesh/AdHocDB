import pandas as pd

# STARS INDEX
# creates an index of business_id's by star ratings in ascending order (1->5)
def setIndex_stars():
    global stars
    stars = pd.read_csv('review-1m.csv')
    stars = stars[['stars', 'business_id']]
    stars = stars.set_index('stars')
    stars = stars.sort_index()

# returns an arraylist of business_id's with stars rating of VAL
def getIDs_stars(val):
    input = "index == " + str(val)
    stars_index = stars.query(input)
    return stars_index.values

# CITY INDEX
# creates an index of business_id's by city in ascending order (#->Z)
def setIndex_city():
    global city
    city = pd.read_csv('business.csv')
    city = city[['city', 'business_id']]
    city = city.set_index(['city'])
    city = city.sort_index()
    city = city.reindex()
    # print(city.head())
    # city_state.loc[('Phoenix')]

# returns an arraylist of business_id's in exact city
# make sure to INPUT using "" ie. getIDs_city("Champaign")
def getIDs_city(input):
    city_index = city.loc[input]
    # print(city_index.head())
    return city_index.values

# STATE INDEX
# creates an index of business_id's by state in ascending order (01->ZET)
def setIndex_state():
    global state
    state = pd.read_csv('business.csv')
    state = state[['state', 'business_id']]
    state = state.set_index(['state'])
    state = state.sort_index()
    # state = state.reindex()
    # print(state.index())

# returns an arraylist of business_id's in exact state
# make sure to INPUT using "" ie. getIDs_state("IL")
def getIDs_state(input):
    state_index = state.loc[input]
    return state_index.values

# NAME INDEX
# creates an index of business_id's by business name in ascending order
def setIndex_name():
    global name
    name = pd.read_csv('business.csv')
    name = name[['name', 'business_id']]
    name = name.set_index(['name'])
    name = name.sort_index()
    # print(name.head())

# returns an arraylist of business_id's with exact business name in ascending order
# make sure to INPUT using "" ie. getIDs_name("Sushi Ichiban")
def getIDs_name(input):
    name_index = name.loc[input]
    return name_index.values

# POSTAL INDEX
# creates an index of business_id's by postal code in ascending order
def setIndex_postal():
    global postal
    postal = pd.read_csv('business.csv')
    postal = postal[['postal_code', 'business_id']]
    postal = postal.set_index(['postal_code'])
    postal = postal.sort_index()
    # print(name.head())

# returns an arraylist of business_id's in exact postal code
# make sure to INPUT using "" ie. getIDs_postal("61820")
def getIDs_postal(input):
    postal_index = postal.loc[input]
    return postal_index.values

# PHOTOS INDEX
# data = pd.read_csv('photos.csv', usecols=['label'], iterator=True, chunksize=50000)

# for chunk in data = pd.read_csv('photos.csv', usecols=['label', 'business_id'], iterator=True, chunksize=50000, error_bad_lines=False):
#
#     chunk.to_csv('photos_sorted.csv', mode='a')
#     chunk = chunk[(chunk.label == "True")]
#     chunk = chunk['business_id']
#     chunk.to_csv('photos_sorted.csv', mode='a')
#
# photos_index = pd.read_csv('photos_sorted.csv')
# photos_sorted = photos.query('label == "True"')
# print(photos_sorted.head())

import pandas as pd

# STARS INDEX
data = pd.read_csv('review-1m.csv')
stars = data[['stars', 'business_id']]
stars = stars.set_index('stars')
stars = stars.sort_index(inplace=True)
stars.index.set_names("stars")
stars_group = stars.groupby(level='stars')
stars.query('index == "1"')

# CITY/COUNTRY INDEX
# make two separate index
data = pd.read_csv('business.csv')
city_state = data[['city', 'country', 'business_id']]
city_state = city_state.set_index(['city', 'state'])
city_state = city_state.sort_index()
city_state = city_state.reindex()
city_state.loc[('Phoenix', 'AZ')]

# NAME/POSTAL CODE INDEX
# make two separate index
data = pd.read_csv('business.csv')
name_zipcode = data[['name', 'postal_code', 'business_id']]
name_zipcode = name_zipcode.set_index(['name','postal_code'])
name_zipcode = name_zipcode.sort_index()
name_zipcode = name_zipcode.reindex()
name_zipcode.loc[('Sushi Ichiban', '61820')]

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

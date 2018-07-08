import pandas

tables = ['movies']
columns = ['movie_title','actor_1_name','actor_1_facebook_likes']
#where = [[actor_1_facebook_likes < 20000],

#for table in tables:
fd = pandas.read_csv('movies.csv')
print(fd.loc[fd['actor_1_facebook_likes'] > 20000])
#print(fd.dtypes)

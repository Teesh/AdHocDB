import pandas
import time

PATH = "../Datasets/Movies/";
#SELECT
columns = ["Name","Film","director_name","Year"]
#FROM
tables = ["oscars", "movies"]
#WHERE
conds = ["oscars.Year >= '2014'","movies.movies_title + 100 = oscars.Film"]

start = time.time()
movies = pandas.read_csv("Datasets/Movies/movies.csv")
oscars = pandas.read_csv("Datasets/Movies/oscars.csv")
# movies = pandas.read_csv("Datasets/Movies/movies.csv", names=["movie_title", "title_year"])
# oscars = pandas.read_csv("Datasets/Movies/oscars.csv", names=["Film", "Year"])

movies['tmp'] = 1
oscars['tmp'] = 1

pandas.merge(movies, oscars, on='tmp')

print(time.time() -start)

#
# for table in tables:
# 	globals()[table] = eval('pandas.read_csv("' + PATH + table + '.csv")["Year", "movies"]')
#
# for table in tables:


'''
table1 = 'movies'
table2 = 'oscars'
l_col = 'movies_title'
r_col = 'Film'
out = eval(table1+'.merge('+table2+', left_on="'+l_col+'", right_on="'+r_col+'")
out = oscars.merge(movies, left_on='Film', right_on='movie_title', how='inner')
print(out[columns])
'''

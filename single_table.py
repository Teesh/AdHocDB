import pandas

PATH = "../Datasets/Movies/";
#SELECT
columns = ["Name","Film","director_name","Year"]
#FROM
tables = ["oscars", "movies"]
#WHERE
conds = ["oscars.Year >= '2014'","movies.movies_title + 100 = oscars.Film"]

#oscars = pandas.read_csv("../Datasets/Movies/oscars.csv") 
for table in tables:
	globals()[table] = eval('pandas.read_csv("' + PATH + table + '.csv")')

for table in tables:
	

'''
table1 = 'movies'
table2 = 'oscars'
l_col = 'movies_title'
r_col = 'Film'
out = eval(table1+'.merge('+table2+', left_on="'+l_col+'", right_on="'+r_col+'")
out = oscars.merge(movies, left_on='Film', right_on='movie_title', how='inner')
print(out[columns])
'''

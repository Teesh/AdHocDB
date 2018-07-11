import pandas

tables = ['movies']
columns = ['movie_title','actor_1_name','actor_1_facebook_likes']
conds = ["movies.movie_title == 'King Kong'","movies.actor_1_facebook_likes < 20000","actor_2_facebook_likes > actor_1_facebook_likes","movies.title_year < 2010"]

#for table in tables:
movies = pandas.read_csv('../Datasets/Movies/movies.csv')

#list(fd)

#print(movies[pandas.Series(conds[0])]);
#type_var = eval(conds[0]);
#print(type_var.to_string())

print(movies[eval(conds[1]) & eval(conds[3])])
#print(movies[movies.movies_title == "King_Kong"])
#print(movies[movies.title_year + 10 < 2018])
#print(movies.loc[movies["movie_title"] == "King Kong"])
#print(movies[movies.actor_1_facebook_likes > 20000])
#print(movies.dtypes)

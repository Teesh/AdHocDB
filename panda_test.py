import pandas

tables = ['movies']
columns = ['movie_title','actor_1_name','actor_1_facebook_likes']
where = ['actor_1_facebook_likes < 20000','and','id=1']

def query_plan():
	for i in where:
		parse the clause
		function(leftside, operator, rightside)

def function(leftside, operator, rightside):
	if leftside contains an operator
		parse leftside

#for table in tables:
fd = pandas.read_csv('movies.csv')
print(fd.loc[fd['actor_1_facebook_likes'] > fd.loc[fd['actor_2_facebook_likes']])
#print(fd.dtypes)

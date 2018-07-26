import argparse
import sqlparse
import pandas
from sqlparse.sql import *
from sqlparse.tokens import *
import re
import time
from collections import defaultdict

import helper_func
import query_plan
import preprocess
import parse

#movies = pandas.read_csv('./movies.csv')

#print(movies[eval(conds[1]) & eval(conds[3])])

path = "./Datasets/Movies/"

def main():
    parser = argparse.ArgumentParser(description='Preprocessing files and running queries.')
    parser.add_argument('--preprocess', metavar = 'Files', nargs='+', type=str,
                      help='Preprocess files where files are given in a space seperated list. Ex. File1 File2 ...')
    parser.add_argument('--query', type=str,  nargs='+',
                      help='Run a query with the syntax "SELECT <columns> FROM <tables> WHERE <condition>". Note: The quotations around the SELECT statement are very important.')

    args = parser.parse_args()
    query = args.query
    preprocess = args.preprocess
    if preprocess is None:
        #go to query function since no preprocessing needed
        query = ' '.join(query)
        parse.parsing(query)

    if query is None:
        #go to preprocessing function
        preprocess.preprocessing()










if __name__ == "__main__":
    main()

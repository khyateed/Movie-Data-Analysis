import gzip
import os
import pandas as pd
import sqlite3
import sys, getopt

def save_base_files(sql=False):
    data, table_names = unzip_base_data()
    save_files(data, table_names, sql=sql)
    
def save_level_up_files(sql=False):
    data, table_names = unzip_level_up_data()
    save_files(data, table_names, sql=sql)


def unzip_base_data():
    base_files = ['imdb.title.basics.csv.gz', 'imdb.title.ratings.csv.gz', 'bom.movie_gross.csv.gz']
    data = unzip_files(base_files)
    table_names = {'imdb.title.basics.csv.gz': 'imdbTitleBasics', 
                   'imdb.title.ratings.csv.gz': 'imdbTitleRatings',
                   'bom.movie_gross.csv.gz': 'bomMovieGross'}
    
    return data, table_names
    
    
def unzip_level_up_data():
    level_up_files = ['imdb.title.crew.csv.gz',
                     'tmdb.movies.csv.gz',
                     'imdb.title.akas.csv.gz',
                     'imdb.name.basics.csv.gz',
                     'rt.reviews.tsv.gz',
                     'rt.movie_info.tsv.gz',
                     'tn.movie_budgets.csv.gz',
                     'imdb.title.principals.csv.gz']
    
    data = unzip_files(level_up_files)
    
    table_names = {'imdb.title.crew.csv.gz':'imdbTitleCrew',
                    'tmdb.movies.csv.gz': 'tmdbMovies',
                     'imdb.title.akas.csv.gz':'imdbTitleAkas',
                     'imdb.name.basics.csv.gz':'imdbNameBasics',
                     'rt.reviews.tsv.gz':'imdbReviews',
                     'rt.movie_info.tsv.gz':'rtMovieInfo',
                     'tn.movie_budgets.csv.gz':'tnMovieBudgets',
                     'imdb.title.principals.csv.gz':'imdbTitlePrincipals'}
    
    return data, table_names



def unzip_files(file_names):
    zipped_data_path =  'zippedData' + os.path.sep
    data = {}
    for file in file_names:
        f = gzip.open(zipped_data_path + file, 'rb')
        try:
            data[file] = pd.read_csv(f)
        except pd.errors.ParserError:
            data[file] = pd.read_csv(f, delimiter='\t', encoding='latin')
            
        f.close()
    return data

    
def load_files_into_sql(data, table_names):
    data_path = 'data' + os.path.sep
    db_path = data_path + 'movies.db'
    conn = sqlite3.connect(db_path)
    for key in data:
        data[key].to_sql(table_names[key], conn, index=False, if_exists='replace')
        

def save_csv(data, table_names):
    data_path = 'data' + os.path.sep
    for key in data:
            data[key].to_csv(data_path + table_names[key] + '.csv')
    
def save_files(data, table_names, sql=False):
    
    if not os.path.isdir('data'):
        os.mkdir('data')
        
    if sql:
        load_files_into_sql(data, table_names)

    else:
        save_csv(data, table_names)
        

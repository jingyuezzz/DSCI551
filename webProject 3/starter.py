from flask import Flask
from flask import render_template
from flask import request, redirect, url_for
import pandas as pd
import sys
from unittest import result
import requests
import json
import sqlalchemy
import pymysql
import pymysql.cursors
from sqlalchemy.engine import cursor
import csv
import re
from sqlalchemy import create_engine
import pandas as pd
import time

# import pyodbc
app = Flask(__name__)
app.url_map.strict_slashes = False
url = 'https://project-1-e1f5c-default-rtdb.firebaseio.com'

# connect to mysqldb
hostname = "localhost"
dbname = "data"
uname = "root"
pwd = "2800t123"
# Create SQLAlchemy engine to connect to MySQL Database
engine = create_engine(
    "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
        host=hostname, db=dbname, user=uname, pw=pwd)
)
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='2800t123',
                             database='data')
cursors = connection.cursor()


def mkdir(path):
    a = path.split(" ")
    response = requests.put(url + a[1] + '.json', json={'read_me': 0})
    if response.status_code == 200:
        return a[1] + " " + "create successfully"
    return response


def ls(path):
    a = path.split(" ")
    response = requests.get('https://project-1-e1f5c-default-rtdb.firebaseio.com/' + a[1] + '.json')
    answer = json.loads(response.text)
    list1 = []
    for item in list(answer.keys()):
        list1.append(item)
    if len(list1) > 1:
        list1.remove('read_me')
    return list1


def cat(path):
    first = path.split(" ")
    a = first[1].split("/")
    name = a[-1].split(".")[0]
    sql = "select * from " + name + ";"
    cursors.execute(sql)
    results = cursors.fetchall()
    df = pd.DataFrame(results)
    df.columns = [x[0] for x in cursors.description]
    df1 = df.iloc[:, 1:]
    return df1.to_html()


def check(path):
    results = requests.get('https://project-1-e1f5c-default-rtdb.firebaseio.com/' + path + '.json')
    result_dict = json.loads(results.text)
    if result_dict is None:
        return False
    else:
        return True


def rm(path):
    first = path.split(" ")
    name = first[-1].split(".")[0]
    if not check(name):
        return 'No such directory'
    response = requests.delete(url + name + '.json', json={'read_me': 0})
    if response.status_code == 200:
        return first[-1] + " " + "delete successfully"
    return response

def check(path):
    results = requests.get('https://project-1-e1f5c-default-rtdb.firebaseio.com/' + path + '.json')
    result_dict = json.loads(results.text)
    if result_dict is None:
        return False
    else:
        return True


def put(path):
    # check if path exists
    path_list = path.split(" ")
    if len(path_list) < 3:
        return 'Please enter function and two parameters'
    if not check(path_list[2]):
        return 'No such directory'
    # upload metadata to Firebase
    file_name = path_list[1].split('.')[0]
    str_name = str(path_list[1])
    df = pd.read_csv(path_list[1], low_memory=False)
    data = df.dropna()
    zipcode = data.iloc[:, -1:].columns
    zipcodes = data[zipcode].squeeze().unique()
    n = 1
    zip_file = {}
    b = 0
    for z in zipcodes:
        res = file_name + str(int(z))
        a = str(int(z))
        zip_file[a] = res
        n += 1
        b += 1
    requests.put('https://project-1-e1f5c-default-rtdb.firebaseio.com' + path_list[2] + '/' + file_name + '.json',
                 json=zip_file)

    list = ls(path)

    hostname = "localhost"
    dbname = "data"
    uname = "root"
    pwd = "2800t123"
    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine(
        "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
            host=hostname, db=dbname, user=uname, pw=pwd)
    )

    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='2800t123',
                                 database='data')

    cursors = connection.cursor()
    # upload data to mysql
    df = data.drop(data.columns[[0]], axis=1)
    file_name = path_list[1].split('.')[0]
    df_name = {}
    cursors.execute('DROP TABLE IF EXISTS ' + file_name + ';')
    data.to_sql(file_name, engine, index=False)

    for i in range(0, len(df.iloc[:, -1].unique())):
        zipcode = df.iloc[:, -1].unique()[i]
        name = file_name + str(int(zipcode))
        df_name[name] = df.loc[df.iloc[:, -1] == zipcode]
    a = 0
    for key, value in df_name.items():
        cursors.execute('DROP TABLE IF EXISTS ' + key + ';')
        a += 1
        value.to_sql(key, engine, index=False)
        print('Creating table ' + key + '....')
        print(key + ' table is created')
    connection.commit()
    # cursors.close()
    connection.close()
    engine.dispose()

    return 'Total number of partitions:' + str(a) + ' was created'

def getPartitionLocations(file):
    first = file.split(" ")
    a = first[1].split(".")
    # name = a[-1].split(".")[0]
    if not check(a[0]):
        return ('no such directory')
    response = requests.get('https://project-1-e1f5c-default-rtdb.firebaseio.com/' + a[0] + '.json')
    location = json.loads(response.text)
    return list(location.keys())


def readPartition(path):
    first = path.split(" ")
    file = first[1].split(".")[0]
    partition_num = first[2]
    if not check(file):
        return 'no such directory'
    response = requests.get('https://project-1-e1f5c-default-rtdb.firebaseio.com/' + file + '.json')
    location = json.loads(response.text)
    output = location[partition_num]

    sql = "select * from " + output + ";"

    cursors.execute(sql)
    results = cursors.fetchall()
    df = pd.DataFrame(results)
    df.columns = [x[0] for x in cursors.description]
    df1 = df.iloc[:, 1:]
    #print(df.columns)
    return df


def getZip(file):
    file = file.split('.')[0]
    response = requests.get('https://project-1-e1f5c-default-rtdb.firebaseio.com/' + file + '.json')
    if response.text == "null":
        return "error"
    location = json.loads(response.text)
    list1 = []
    for key, values in location.items():
        list1.append(key)
    return list1


def mapPartition(file, p):
    zips = getZip(file)
    if zips == "error":
        return "wrong path"
    if p not in zips:
        return ('zipcode ' + str(p) + ' is not included in the database')

    df = readPartition("read" + " " + file + " " + str(p))
    name = file.split('/')[-1]
    if name == 'crime.csv':
        # count every type of crime
        d1 = df.groupby(['Crm_Cd_Desc'])['DR_NO'].count().reset_index(name='Count').sort_values(['Count'],
                                                                                                ascending=False) \
            .reset_index().drop(['index'], axis=1)
        # number of crime for every zip code
        d2 = df.groupby(['ZipCode'])['DR_NO'].count().reset_index(name='Count').sort_values(['Count'], ascending=False) \
            .reset_index().drop(['index'], axis=1)

        # select ZipCode, count(*) from p where Crm_Cd_Desc = 'THEFT, PERSON' group by ZipCode;
        a = df[['ZipCode', 'Crm_Cd_Desc']]
        d3 = a[a['Crm_Cd_Desc'] == 'THEFT, PERSON'].groupby(['ZipCode']).count().reset_index()
        d3.columns = ['Crm_Cd_Desc', 'count']

        # select Zipcode,Crm_Cd_Desc, count(*) from crime90007 where Crm_Cd_Desc in ['BIKE - STOLEN', 'ROBBERY']
        # group by ZipCode;
        # select Zipcode, count(*) from crime90007 where Crm_Cd_Desc like '%BIKE - STOLEN%' or Crm_Cd_Desc like
        # '%ROBBERY%' group by ZipCode;
        a = df[['ZipCode', 'Crm_Cd_Desc']]
        d4 = a[a['Crm_Cd_Desc'].isin(['BIKE - STOLEN', 'ROBBERY'])].groupby(['ZipCode']).count().reset_index()
        d4.columns = ['Crm_Cd_Desc', 'count']

        # 90007, 90089 check usc around and find the maximum number of crime:
        # if df['ZipCode'][0] == 90007 or df['ZipCode'][0] == 90089:
        #     d5 = df.groupby(['Crm_Cd_Desc'])['DR_NO'].count().reset_index(name='Count').sort_values(['Count'],
        #                                                                                             ascending=False). \
        #         reset_index()
        #     max_crime = d5['Crm_Cd_Desc'][0]
        #     return [d1, d2, d3, d4, d5]
        # print(d2)
        return [d1, d2, d3, d4]
    if name == 'gro.csv':
        # number of grocery store in every zip code
        d1 = df.groupby(['zip'])['DBA_NAME'].count().to_frame().reset_index()
        d1 = d1.rename(columns={"DBA_NAME": "DBA_COUNT"})
        a = int(d1['zip'])
        d1['zip'] = a

        # common grocery store in every zip code
        zip_code = df['zip'].iloc[0]
        # count1 = len(df.groupby('DBA_NAME')['DBA_NAME'].count())
        # lst1 = [zip_code] * count1
        d2 = df.groupby(['DBA_NAME']).count().reset_index()
        d2 = d2['DBA_NAME'].to_frame()
        d2['zip_code'] = int(zip_code)
        d2 = d2[['zip_code', 'DBA_NAME']]
        # print(d2)
        return [d1, d2]
    if name == 'yelp.csv':
        # number of restaurants in every zip code
        d1 = df.groupby('zip')['name'].count().to_frame().rename(columns={'name': 'count'}).reset_index()

        # average overall rating in every zip code
        d2 = df.groupby('zip')['overall_rating'].mean().to_frame().reset_index()

        # 5 most review_count restaurants in every zip code
        d3 = df.sort_values(by='review_count', ascending=False).head(5).reset_index(drop=True).drop(['id'],
                                                                                                    axis=1)
        d3 = d3[['zip', 'name', 'review_count', 'overall_rating', 'address']]

        # 5 highest rating restaurants in every zip code
        d4 = df.sort_values(by='overall_rating', ascending=False).head(5).reset_index(drop=True).drop(['id', 'review_count'],
                                                                                              axis=1)
        d4 = d4[['zip', 'name', 'overall_rating', 'address']]
        # if d4.shape[0] < 5:
        #     print('len', d4.shape[0])

        # restaurants that have the most review_counts with same rating in every zip code
        d5 = df.groupby('overall_rating').agg('max').reset_index().drop(['id'], axis=1)
        d5 = d5[['zip', 'name', 'overall_rating', 'review_count', 'address']]
        return [d1, d2, d3, d4, d5]
    else:
        return 'no table'


def reduceMap(file):
    # find all partitions for file
    filepath = "na" + " " + file
    lists = getPartitionLocations(filepath)
    # print(lists)
    dict1 = {}

    for x in lists:
        # print(x)
        df_lists = mapPartition(file, x)
        dict1[x] = df_lists
    # print('dict1 done')
    # write specific methods for each file
    name = file.split('/')[-1]
    if name == 'crime.csv':
        d1 = pd.DataFrame()
        d2 = pd.DataFrame()
        d3 = pd.DataFrame()
        d4 = pd.DataFrame()

        # count every type of crime
        for keys, values in dict1.items():
            d1 = pd.concat([d1, values[0]], ignore_index=True)
            d2 = pd.concat([d2, values[0]], ignore_index=True)
            d3 = pd.concat([d3, values[0]], ignore_index=True)
            d4 = pd.concat([d4, values[0]], ignore_index=True)

        d1 = d1.groupby('Crm_Cd_Desc').sum().sort_values(by='Count', ascending=False).reset_index()
        # print('Dataframe from crime done')
        return [d1, d2, d3, d4]
    elif name == 'gro.csv':
        d1 = pd.DataFrame()
        d2 = pd.DataFrame()
        for keys, values in dict1.items():
            d1 = pd.concat([d1, values[0]], ignore_index=True)
            d2 = pd.concat([d2, values[1]], ignore_index=True)
            # d3 = pd.concat([d3, values[2]], ignore_index=True)
        # print('Dataframe from grocery done')
        return [d1, d2]
    elif name == 'yelp.csv':
        d1 = pd.DataFrame()
        d2 = pd.DataFrame()
        d3 = pd.DataFrame()
        d4 = pd.DataFrame()
        d5 = pd.DataFrame()
        for keys, values in dict1.items():
            d1 = pd.concat([d1, values[0]], ignore_index=True)
            d2 = pd.concat([d2, values[1]], ignore_index=True)
            d3 = pd.concat([d3, values[2]], ignore_index=True)
            d4 = pd.concat([d4, values[3]], ignore_index=True)
            d5 = pd.concat([d5, values[4]], ignore_index=True)
        # print('Dataframe from yelp done')
        return [d1, d2, d3, d4, d5]


@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == "POST":
        # getting input with  in HTML form
        res = request.form.get("command")
        res_plus = res.replace("/", "+")
        return redirect(url_for('result', req=res_plus))
    return render_template('LA database.html')


@app.route('/result/<req>', methods=['GET'])
def result(req):
    path = req.replace("+", "/")
    command = req.split(" ")
    if command[0] == 'mkdir':
        out = mkdir(path)
    elif command[0] == 'ls':
        out = ls(path)
    elif command[0] == 'cat':
        out = cat(path)
        return out
    elif command[0] == 'rm':
        out = rm(path)
    elif command[0] == 'put':
        out = put(path)
    elif command[0] == 'getPartitionLocations':
        out = getPartitionLocations(path)
    elif command[0] == 'readPartition':
        out = readPartition(path)
        if type(out) != str:
            return out.to_html()
    else:
        out = "wrong command!"
    return render_template('result.html', data=out)

@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == "POST":
        if request.form['action'] == 'Search':
            filepath = request.form.get("filepath")
            zipcode = request.form.get("zipcode")
            file = filepath.replace("/", "-")
            opt_zip = file + "+" + str(zipcode) + "+" + "search"
            return redirect(url_for('searchres', optcode=opt_zip))
        elif request.form['action'] == 'MapReduce':
            filepath = request.form.get("filepath")
            file = filepath.replace("/", "-") + "+" + "reduce"
            return redirect(url_for('searchres', optcode=file))
    return render_template('search.html')


@app.route('/searchres/<optcode>', methods=['GET'])
def searchres(optcode):
    file = optcode.split("+")
    if file[-1] == "reduce":
        path = file[0].replace("-", "/")
        result = reduceMap(path)
        name = path.split("/")[-1]
        lists_tables = [x.to_html(classes='data', index=False) for x in result]
        if name == 'gro.csv':
            return render_template('searchres.html',
                                   tables=lists_tables, titles=['na', 'number of Grocery Stores at LA Districts',
                                                                'Grocery Stores at LA Districts'])
        if name == 'crime.csv':
            return render_template('searchres.html',
                                   tables=lists_tables, titles=['na', 'count type of crimes at LA Districts',
                                                                'number of crimes at LA Districts',
                                                                'number of theft crimes at LA Districts',
                                                                'number of bike stole and robbery crimes at LA '
                                                                'Districts '
                                                                ])
        if name == 'yelp.csv':
            return render_template('searchres.html',
                                   tables=lists_tables, titles=['na', 'number of restaurants at LA Districts',
                                                                'average overall ratings of restaurants at LA Districts',
                                                                '5 most review_count restaurants for every zip code '
                                                                'at LA Districts',
                                                                '5 highest rating restaurants for every zip code '
                                                                'at LA Districts',
                                                                'restaurants that have the most review_counts '
                                                                'with same rating for every zip code '
                                                                'at LA Districts'])

    elif file[-1] == "search":
        path = file[0].replace("-", "/")
        zip = file[1]
        name = path.split("/")[-1]

        if name == 'gro.csv':
            result = mapPartition(path, zip)
            if type(result) == str:
                return render_template('result.html', data=result)
            lists_tables = [x.to_html(classes='data', index=False) for x in result]
            return render_template('searchres.html',
                                   tables=lists_tables, titles=['na', 'number of Grocery Stores at ' + zip,
                                                                'Grocery Stores at ' + zip])
        elif name == 'crime.csv':
            result = mapPartition(path, zip)
            if type(result) == str:
                return render_template('result.html', data=result)
            lists_tables = [x.to_html(classes='data', index=False) for x in result]
            return render_template('searchres.html',
                                   tables=lists_tables, titles=['na', 'count type of crimes at ' + zip,
                                                                'number of crimes at ' + zip,
                                                                'number of theft crimes at '
                                                                + zip,
                                                                'number of bike stole and robbery crimes at ' + zip])
        elif name == 'yelp.csv':
            result = mapPartition(path, zip)
            if type(result) == str:
                return render_template('result.html', data=result)
            lists_tables = [x.to_html(classes='data', index=False) for x in result]
            return render_template('searchres.html',
                                   tables=lists_tables, titles=['na', 'number of restaurants at ' + zip,
                                                                'average overall ratings ' + zip,
                                                                '5 most review_count restaurants ' + zip,
                                                                '5 highest rating restaurants in every zip code ' + zip,
                                                                'restaurants that have the most review_counts with same rating ' + zip])
        else:
            data = "wrong file name"
            return render_template('result.html', data=data)


if __name__ == '__main__':
    app.run()

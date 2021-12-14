from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import json
import pandas
import csv
import sys
from preprocess import stopwords
from preprocess import *
import numpy as np

from NBClassifier import NB_classifier
from RfClassifier import Rf_classifier
from KNNClassifier import knn_classifier
from k_means_clustering import clustering
from preprocess import preprocess
from Testing import testing

if sys.argv[1] == 'train':
	schema = ["NB","RF","KNN"]
	with open("training.csv",'w',newline='') as file:
		writer = csv.writer(file)
		writer.writerow(schema)

if sys.argv[1]=='cluster':
	schema=['centroid1_diff', 'centroid2_diff']
	with open("clustering.csv",'w',newline='') as file1:
		writer = csv.writer(file1)
		writer.writerow(schema)

clust1 = np.array([0,0])
clust2=  np.array([0,0])

sc = SparkContext("local[2]","stream")
ssc = StreamingContext(sc, 1)
spark = SparkSession.builder.getOrCreate()

def makeDict(rdd):
    print(rdd)
    return json.loads(rdd)

stopwords_english = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"]


def main(rdd): 
    data_rdd = rdd.collect()
    
    if len(data_rdd) != 0:
        print(data_rdd)
        df= pandas.DataFrame(data_rdd[0])
        df = df.transpose()
        df = preprocess(df)

        tfidfvectorizer = TfidfVectorizer(max_features=1000, min_df=5, max_df=0.7, stop_words=stopwords_english)  
        x = tfidfvectorizer .fit_transform(df['feature1']).toarray()
        y = df['feature0'].apply(lambda x:int(x))
        x_train, x_test, y_train, y_test = train_test_split(x,y,test_size = 0.07, random_state =9510)
        if sys.argv[1] == 'train' :
            Naive_bayes = NB_classifier(x_train, x_test, y_train, y_test)
            Random_forest = Rf_classifier(x_train, x_test, y_train, y_test)
            KNN_classification = knn_classifier(x_train, x_test, y_train, y_test)

            with open("training.csv",'a',newline='') as file:
                    writer = csv.writer(file)
                    data=[Naive_bayes, Random_forest, KNN_classification]
                    writer.writerow(data)
        
        elif sys.argv[1] == 'cluster' :
            global clust1
            global clust2
            distx,disty = clustering(x_train)
            dist1= np.linalg.norm(distx - clust1)
            dist2= np.linalg.norm(disty- clust2)
            data = [dist1,dist2]
            with open("clustering.csv",'a',newline='') as file1:
                writer = csv.writer(file1)
                writer.writerow(data)
        
        else :
            testing(x,y)


lines = ssc.socketTextStream("localhost", 6100)
lines.map(makeDict).foreachRDD(main)
		
ssc.start()
ssc.awaitTermination()
ssc.stop()

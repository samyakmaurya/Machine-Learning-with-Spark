from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import pandas as pd
from nltk.stem import PorterStemmer

import re                                  
import string                             
import nltk

#print(df)

#Preprocesing:

def punc(x):
    x= x.lower()
    x = re.sub(r'^RT[\s]+', '.' , x)
    x = re.sub('((www\.[^\s]+)|(https?://[^\s]+))','url',x)
    x = re.sub(r'#', '' , x)
    x = re.sub(r'[0-9]', '.' , x)
    x = re.sub(r'(\\u[0-9A-Fa-f]+)', '.', x)       
    x = re.sub(r'[^\x00-\x7f]' , '.',x)
    x = re.sub('@[^\s]+','atUser',x)
    x = re.sub(r"(\!)\1+", ' multiExclamation', x)
    x = re.sub(r"(\?)\1+", ' multiQuestion', x)
    x = re.sub(r"(\.)\1+", ' multistop', x)
    return x

def tokens(x):
    return x.split()

def stopwords(x):
    stopwords = nltk.corpus.stopwords.words('english')
    return [i for i in x if i not in stopwords]

stems = PorterStemmer()

def stem(x):
    res=[]
    for word in x:
        stem_word= stems.stem(word)
        res.append(stem_word)
    return " ".join(res)



def preprocess(df):
    df['feature1']= df['feature1'].apply(lambda x: punc(x))
    df['feature1']= df['feature1'].apply(lambda x: tokens(x))
    df['feature1']= df['feature1'].apply(lambda x: stopwords(x))
    df['feature1']= df['feature1'].apply(lambda x: stem(x))
    return df

#print("Your data has been processed and is ready for modelling")
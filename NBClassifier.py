import pandas as pd
import numpy as np

from sklearn.naive_bayes import BernoulliNB
from sklearn.metrics

import csv

def NB_classifier(train_X, test_X, train_Y, test_Y):
    clsf = BernoulliNB()
    clsf.fit(train_X,train_Y)
    y_pred = clsf.predict(test_X)
    score = metrics.accuracy_score(test_Y,y_pred)
    report = metrics.classification_report(test_Y,y_pred,target_names= ['Positive','Negative']
    return (clsf,score,report)

import sklearn
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

#points = [[0,0],[1,1],[0,1],[1,0],[10,10],[10,12]]

def clustering(points):
    kmeans = KMeans(n_clusters = 2)

    #kmeans.fit(points)

    identified_clusters = kmeans.fit_predict(points)


    return identified_clusters	#returns which cluster a particular point is in

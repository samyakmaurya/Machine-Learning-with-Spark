import sklearn
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import classification_report,confusion_matrix


def knn_classifier(train_X, test_X, train_Y, test_Y):
	
	#uncomment and add how many neighbors we want
	#n_neighbors = 

	classifier = KNeighborsClassifier(n_neighbors)	
	classifier.fit(train_X,train_Y)

	y_pred = classifier.predict(test_X)

	report = classification_report(test_Y,y_pred)
	score = metrics.accuracy_score(test_Y,y_pred)

	return (classifier,score,report)


import sklearn.metrics

def Rf_classifier(train_X, test_X, train_Y, test_Y):
	clfr=RandomForestClassifier(max_depth=2, random_state=0)
	clfr.fit(X_train,y_train)
	y_pred=clfr.predict(X_test)
	score = metrics.accuracy_score(test_Y,y_pred)
    rep = metrics.classification_report(test_Y,y_pred,target_names= ['Positive','Negative'])
    return(clfr,score,rep)
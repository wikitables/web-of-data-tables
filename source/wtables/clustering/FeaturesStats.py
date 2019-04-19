import pandas as pd
import numpy as np
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import f1_score
import warnings
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.metrics import classification_report
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
import pickle
import gzip
import sys
import math

def getDataChunks(file, sep, decimalPoint, fileMax, fileMin):
    chunksize= 10**5
    columns_s = [str(i) for i in np.arange(1, 73)]
    maxValues = {str(k):v for k, v in eval(open(fileMax, "r").read()).items()}
    minValues = {str(k):v for k, v in eval(open(fileMin, "r").read()).items()}
    for chunk in pd.read_csv(file, chunksize=chunksize, names=columns_s,
                             sep=sep, decimal=decimalPoint,index_col=False):
        x = chunk.iloc[:, 0:64]
        xf=prepareDataNormalized(x, maxValues, minValues)

        X = xf.values
        print('predict')
        print(xf.columns)
        #print("shape: ", X.shape)
        #print('max',xf.max())
        yield X

def normalize(xf, maxValues, minValues):
    result = xf.copy()
    #maxValues=maxValues.to_dict()
    #minValues=minValues.to_dict()

    for feature_name in xf.columns:

        max_value = maxValues[str(feature_name)]
        min_value = minValues[str(feature_name)]
        if max_value==0:
            result[feature_name]=0
        else:
            result[feature_name] = (xf[feature_name] - min_value) / (max_value - min_value)
    return result

def prepareDataNormalized(x, maxValues, minValues):

    noNormalized=['2', '3', '4', '5','7','8', '26', '27', '37', '39', '40', '45', '46', '64']
    noImportant=['21','22','23','38','43','44','63']
    r1 = [str(i) for i in np.arange(2,65) if str(i) not in noNormalized and str(i) not in noImportant]

    x1=x[r1]
    # new = '39','40','45','46'
    x2 = x[noNormalized]
    #normalized_x1 = (x1 - x1.min()) / (x1.max() - x1.min())
    #print('max1: ', maxValues)
    x1 = normalize(x1, maxValues, minValues)
    xf = pd.concat([x1, x2], axis=1, sort=False)

    colnames = {col: int(col) for col in list(xf.columns)}

    xf = xf.rename(columns=colnames)
    xf = xf.sort_index(axis=1)
    print('columns: ', xf.columns)

    return xf


def prepareData(x):
    print('head preparation:')
    print(x.head(2))
    print(x[['60']].head(2))

    noImportant=['21','22','23','38','43','44','63']
    r1 = [str(i) for i in np.arange(2,65) if str(i) not in noImportant]

    xf=x[r1]
    # new = '39','40','45','46'

    colnames = {col: int(col) for col in list(xf.columns)}

    xf = xf.rename(columns=colnames)
    xf = xf.sort_index(axis=1)
    print("final features", xf.columns)

    return xf

def prepareTrainingData(csvTraining, sep, decimalPoint):
    data = pd.read_csv(csvTraining,sep=sep, decimal=decimalPoint, index_col=False)
    data = data[(data['VALIDATION'] < 3) & (data['VALIDATION'] > 0)].reset_index(drop=True)
    #data1 = data[data['ORIGIN'] == 'f']  # 489 #380

    maxValues=data.max()
    minValues = data.min()
    xf=prepareDataNormalized(data, maxValues, minValues)
    print('training: ', xf.columns)
    print('target: ', data.columns[60])
    #print(xf.columns)
    # print(xf.head(2))
    X = xf.values
    y=data.iloc[:,60].values

    return X, y

def trainingModels(X, y):
    warnings.simplefilter(action='ignore', category=FutureWarning)
    kf = StratifiedShuffleSplit(n_splits=5, test_size=0.3)
    p1 = p2 = p3 = p4 = p5 = 0
    r1 = r2 = r3 = r4 = r5 = 0
    f1 = f2 = f3 = f4 = f5 = 0
    cont = 0
    for train_index, test_index in kf.split(X, y):
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = Y[train_index], Y[test_index]
        cl1 = KNeighborsClassifier(n_neighbors=1)
        cl2 = DecisionTreeClassifier()
        cl3 = RandomForestClassifier(max_depth=10, max_features=None, min_samples_leaf=3)
        cl4 = SVC(kernel='linear')
        cl5 = GaussianNB()

        cl1.fit(X_train, y_train)
        y_pred = cl1.predict(X_test)
        p1 += precision_score(y_test, y_pred, average='weighted')
        r1 += recall_score(y_test, y_pred, average='weighted')
        f1 += f1_score(y_test, y_pred, average='weighted')
        print('KNN')
        print(classification_report(y_test, y_pred))


        cl2.fit(X_train, y_train)
        y_pred = cl2.predict(X_test)
        p2 += precision_score(y_test, y_pred, average='weighted')
        r2 += recall_score(y_test, y_pred, average='weighted')
        f2 += f1_score(y_test, y_pred, average='weighted')
        print('Decision Tree')
        print(classification_report(y_test, y_pred))

        cl3.fit(X_train, y_train)
        y_pred = cl3.predict(X_test)
        p3 += precision_score(y_test, y_pred, average='weighted')
        r3 += recall_score(y_test, y_pred, average='weighted')
        f3 += f1_score(y_test, y_pred, average='weighted')
        print('Random')
        print(classification_report(y_test, y_pred))

        # cl4.fit(X_train, y_train)
        # y_pred=cl4.predict(X_test)
        # p4+=precision_score(y_test, y_pred, average='weighted')
        # r4+=recall_score(y_test, y_pred, average='weighted')
        # f4+=f1_score(y_test, y_pred, average='weighted')
        # print('Logistic')
        # print(classification_report(y_test, y_pred))

        cl5.fit(X_train, y_train)
        y_pred = cl5.predict(X_test)
        p5 += precision_score(y_test, y_pred, average='weighted')
        r5 += recall_score(y_test, y_pred, average='weighted')
        f5 += f1_score(y_test, y_pred, average='weighted')
        print('Naive Bayes')
        print(classification_report(y_test, y_pred))

        cont += 1
        # print('Set y: ', [ip for ip in y_test if ip==1])
    print('***  KNN  ***')
    print('Precision: ', p1 / cont)
    print('Recall: ', r1 / cont)
    print('F1-score: ', f1 / cont)
    print('***  Decision Tree  ***')
    print('Precision: ', p2 / cont)
    print('Recall: ', r2 / cont)
    print('F1-score: ', f1 / cont)
    print('***  Random Forest  ***')
    print('Precision: ', p3 / cont)
    print('Recall: ', r3 / cont)
    print('F1-score: ', f3 / cont)
    # print('***  SVC  ***')
    # print('Precision: ', p4/cont)
    # print('Recall: ', r4/cont)
    # print('F1-score: ', f4/cont)
    print('***  Naive Bayes  ***')
    print('Precision: ', p5 / cont)
    print('Recall: ', r5 / cont)
    print('F1-score: ', f5 / cont)

def trainingFinalModel(csvTrainedData, model):

    X, y=prepareTrainingData(csvTrainedData,sep='\t', decimalPoint=',')
    model.fit(X,y)
    filename = 'trainedModel.sav'
    pickle.dump(model, open(filename, 'wb'))

def predictData(model, csvData, fileMax, fileMin):
    #loadedModel = pickle.load(open(fileModel, 'rb'))
    cont=0
    results={n:0 for n in range(2,65)}

    with gzip.open('predictions.csv.gz', 'wt') as fout:
        chunksize = 10 ** 5
        columns_s = [str(i) for i in np.arange(1, 73)]
        maxValues = {str(k): v for k, v in eval(open(fileMax, "r").read()).items()}
        minValues = {str(k): v for k, v in eval(open(fileMin, "r").read()).items()}
        for chunk in pd.read_csv(csvData, chunksize=chunksize, names=columns_s,
                                 sep='\t', decimal='.', index_col=False):
            x = chunk.iloc[:, 0:64]
            xf = prepareDataNormalized(x, maxValues, minValues)

            X = xf.values
            print('predict')
            print(xf.columns)
            print("Chunk : ", cont)
            predictions = model.predict(X)
            predictions=predictions.astype('str')
            fout.write("\n".join(predictions)+"\n")
            cont += 1

def getStd(csvData, sumFile, sep, decimalPoint, n):
    chunksize = 10 ** 5
    columns_s = [str(i) for i in np.arange(1, 73)]
    cont=0
    sum=eval(open(sumFile, 'r').read())
    meanValues = {k: (v / float(n)) for k, v in sum.items()}
    foutf = open('meanN.txt', 'w')
    foutf.write(str(meanValues))
    foutf.close()
    #print('mean values: ', meanValues)
    results={v:0 for v in range(2,65)}
    #for ind in [21,22,23,38,63,43,44]:
    #    del meanValues[ind]
    print(len(meanValues), meanValues)
    maxValues = {str(k): v for k, v in eval(open('max.txt', "r").read()).items()}
    minValues = {str(k): v for k, v in eval(open('min.txt', "r").read()).items()}
    for chunk in pd.read_csv(csvData, chunksize=chunksize, names=columns_s,
                                 sep=sep, decimal=decimalPoint, index_col=False):
        print("Chunk : ", cont)
        cont+=1
        x = chunk.iloc[:, 0:64]
        xf = prepareDataNormalized(x, maxValues, minValues)
        #print(xf.head(2))
        #print(xf.columns)
        #print('shape', xf.shape)
        # r1 features that can be normalized
        diff=xf-meanValues
        diff=np.power(diff,2)
        #print('diff: ', diff)
        frameSum=diff.sum()
        for k, v in results.items():
            results[k]=v+frameSum.get(k,0)
    print(results)
    resultss={k:math.sqrt(v/(float(n)-1)) for k, v in results.items()}
    foutf = open('stdN.txt', 'w')
    foutf.write(str(resultss))
    foutf.close()


def getMinMaxSumTotal(file, sep, decimalPoint):
    chunksize = 10 ** 5
    columns_s = [str(i) for i in np.arange(1, 73)]
    min = {n: 0 for n in range(2, 65)}
    max = {n: 0 for n in range(2, 65)}
    sum = {n: 0 for n in range(2, 65)}
    cont=1
    for chunk in pd.read_csv(file, chunksize=chunksize, names=columns_s,
                                 sep=sep, decimal=decimalPoint, index_col=False):
        print("Chunk : ", cont)

        x = chunk.iloc[:, 0:64]

        # r1 features that can be normalized
        xf=prepareData(x)
        frameSum=xf.sum()
        print("Data")
        print(xf.columns)

        for k, v in sum.items():
            sum[k]=v+frameSum.get(k,0)

        frameMax = xf.max()
        for k, v in max.items():
            newv = frameMax.get(k, 0)
            if v < newv:
                max[k] = newv
        frameMin = xf.min()
        for k, v in min.items():
            newv = frameMin.get(k, 0)
            if cont==1:
                min[k]=newv
            else:
                if v > newv:
                    min[k] = newv
        print("min: ", min)
        cont += 1
    for ind in [21,22,23,38,63,43,44]:
        del min[ind]
        del max[ind]

    foutf = open('min.txt', 'w')
    foutf.write(str(min))
    foutf.close()
    foutf = open('max.txt', 'w')
    foutf.write(str(max))
    foutf.close()
    foutf = open('sum.txt', 'w')
    foutf.write(str(sum))
    foutf.close()
    return min, max


def getMinMaxSum(file, sep, decimalPoint):
    chunksize = 10 ** 5
    columns_s = [str(i) for i in np.arange(1, 73)]
    min = {n: 0 for n in range(2, 65)}
    max = {n: 0 for n in range(2, 65)}
    sum = {n: 0 for n in range(2, 65)}
    for ind in [21,22,23,38,63,43,44]:
        del min[ind]
        del max[ind]
        del sum[ind]
    maxValues = {str(k): v for k, v in eval(open('max.txt', "r").read()).items()}
    minValues = {str(k): v for k, v in eval(open('min.txt', "r").read()).items()}
    cont=1
    for chunk in pd.read_csv(file, chunksize=chunksize, names=columns_s,
                                 sep=sep, decimal=decimalPoint, index_col=False):
        print("Chunk : ", cont)

        x = chunk.iloc[:, 0:64]

        # r1 features that can be normalized
        xf=prepareDataNormalized(x, maxValues, minValues)
        frameSum=xf.sum()
        print("Data")
        print(xf.columns)

        for k, v in sum.items():
            sum[k]=v+frameSum.get(k)

        frameMax = xf.max()
        for k, v in max.items():
            newv = frameMax.get(k)
            if v < newv:
                max[k] = newv
        frameMin = xf.min()
        for k, v in min.items():
            newv = frameMin.get(k)

            if newv==0 and k==61:
                print('Zero', cont)
                #print(xf[xf[61] == 0])
            if cont==1:
                min[k]=newv
            else:
                if v > newv:
                    min[k] = newv
        #print("min: ", min)
        cont += 1


    foutf = open('minN.txt', 'w')
    foutf.write(str(min))
    foutf.close()
    foutf = open('maxN.txt', 'w')
    foutf.write(str(max))
    foutf.close()
    foutf = open('sumN.txt', 'w')
    foutf.write(str(sum))
    foutf.close()
    return min, max

if __name__ == '__main__':

    args = sys.argv[1:]
    option=args[0]
    if option == '0':
        dataFile = args[1]  # '../features_cluster/featuresClusterTest'
        getMinMaxSumTotal(dataFile, "\t", ".")
        print('sum total ok')
    if option=='1':
        trainingFile=args[1] #'../features_cluster/finalFeatures.csv'
        dataFile=args[2]#'../features_cluster/featuresClusterTest'
        #cl3 = RandomForestClassifier(max_depth=10)
        #trainingFinalModel(trainingFile, cl3)
        print('model trainded')
        predictData(cl3,dataFile,'max.txt', 'min.txt')
    else:
        if option=='2':
            dataFile = args[1]#'../features_cluster/featuresClusterTest'
            getMinMaxSum(dataFile, "\t",".")
            print('sum ok')

        if option=='3':
            dataFile = args[1]#'../features_cluster/featuresClusterTest'#
            sumFile="sumN.txt"
            getStd(dataFile, sumFile,"\t",".",args[2])
            print('std ok')

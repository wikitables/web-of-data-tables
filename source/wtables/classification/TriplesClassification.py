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
        yield X

def normalize(xf, maxValues, minValues):
    result = xf.copy()
    for feature_name in xf.columns:
        max_value = maxValues[str(feature_name)]
        min_value = minValues[str(feature_name)]
        if max_value==0:
            result[feature_name]=0
        else:
            result[feature_name] = (xf[feature_name] - min_value) / (max_value - min_value)
    return result

def prepareDataNormalized(x, maxValues, minValues):
    noImportant = ['21', '22', '23', '38', '43', '44']
    noNormalized = ['7', '8', '26', '27', '37', '39', '40', '64']
    r1 = [str(i) for i in np.arange(2,66) if str(i) not in noNormalized and str(i) not in noImportant]
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
    return xf


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
    filename = 'trainedModelRF.sav'
    pickle.dump(model, open(filename, 'wb'))

def predictData(modelFile, csvData, predictionsFile, predictionsProbaFile):
    print(modelFile)
    model = pickle.load(open(modelFile, 'rb'))
    cont=0
    #feature_65 = pd.read_csv('../files/numTablesByCluster.csv.gz', sep="\s+", compression='gzip', index_col=False)
    #feature_65 = feature_65.rename(columns={'1': '65'})
    #features=['4', '5', '6', '7', '8', '11', '12', '13', '14', '17', '18', '19', '20', '24', '25', '30', '31', '32', '34', '36', '41', '42', '47', '48', '49', '50', '53', '54', '61', '62', '63']
    features=['6', '7', '8', '11', '12', '13', '14', '15', '16', '17', '18', '24', '25', '26', '27', '28', '29', '30', '39', '40', '41', '42', '45', '46', '47', '48', '49', '50', '62', '63', '65', 'VALIDATION'] 
    with gzip.open(predictionsProbaFile, 'wt') as fout_proba:
        with gzip.open(predictionsFile, 'wt') as fout:
            chunksize = 10 ** 5
            columns_s = [str(i) for i in np.arange(1, 74)]
            #maxValues = {str(k): v for k, v in eval(open(fileMax, "r").read()).items()}
            #minValues = {str(k): v for k, v in eval(open(fileMin, "r").read()).items()}
            for chunk in pd.read_csv(csvData, chunksize=chunksize, names=columns_s, sep='\t', decimal='.', index_col=False, compression='gzip'):
                print('len features: ', len(features[:-1]))
                X = chunk[features[:-1]]
                predictions = model.predict(X)
                predictions_proba = model.predict_proba(X)
                predictions = predictions.astype('str')
                fout.write("\n".join(predictions)+"\n")
                predictions_proba = predictions_proba.astype('str')
                for pp in predictions_proba:
                    fout_proba.write(str(pp) + "\n")
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
    min = {n: 0 for n in range(2, 66)}
    max = {n: 0 for n in range(2, 66)}
    sum = {n: 0 for n in range(2, 66)}
    cont=1
    for chunk in pd.read_csv(file, chunksize=chunksize, names=columns_s,
                                 sep=sep, decimal=decimalPoint, index_col=False):
        print("Chunk : ", cont)

        x = chunk.iloc[:, 0:64]
        print("Data")
        print(x.head(2))
        # r1 features that can be normalized
        xf=prepareData(x)
        frameSum=xf.sum()



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

    foutf = open('minFinal.txt', 'w')
    foutf.write(str(min))
    foutf.close()
    foutf = open('maxFinal.txt', 'w')
    foutf.write(str(max))
    foutf.close()
    foutf = open('sumFinal.txt', 'w')
    foutf.write(str(sum))
    foutf.close()
    return min, max


def getMinMaxSum(file, sep, decimalPoint):
    chunksize = 10 ** 5
    columns_s = [str(i) for i in np.arange(1, 73)]
    min = {n: 0 for n in range(2, 65)}
    max = {n: 0 for n in range(2, 65)}
    sum = {n: 0 for n in range(2, 65)}
    for ind in [21,22,23,38,43,44]:
        del min[ind]
        del max[ind]
        del sum[ind]
    maxValues = {str(k): v for k, v in eval(open('max.txt', "r").read()).items()}
    minValues = {str(k): v for k, v in eval(open('min.txt', "r").read()).items()}
    cont=1
    for chunk in pd.read_csv(file, chunksize=chunksize, names=columns_s,
                                 sep=sep, decimal=decimalPoint, index_col=False):
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
        dataFile = args[1]
        getMinMaxSumTotal(dataFile, "\t", ".")
    if option=='1':
        dataFile=args[1]
        modelFile=args[2]
        predictionsFile=args[3]
        predictionsProbaFile=args[4]
        predictData(modelFile,dataFile, predictionsFile, predictionsProbaFile)
    else:
        if option=='2':
            dataFile = args[1]
            getMinMaxSumTotal(dataFile, "\t",".")

        if option=='3':
            dataFile = args[1]
            sumFile="sumN.txt"
            getStd(dataFile, sumFile,"\t",".",args[2])

import sympy
import numpy as np

def giveCoordinates(distances):
    distances=np.array(distances)
    print(distances.shape)
    n = len(distances)
    X = sympy.symarray('x', (n, n - 1))

    for row in range(n):
        X[row, row:] = [0] * (n - 1 - row)

    for point2 in range(1, n):

        expressions = []

        for point1 in range(point2):
            expression = np.sum((X[point1] - X[point2]) ** 2)
            expression -= distances[point1, point2] ** 2
            expressions.append(expression)

        X[point2, :point2] = sympy.solve(expressions, list(X[point2, :point2]))[1]

    return X

matrix=[[0,1,2,3],[1,0,5,6],[2,5,0,7],[3,6,7,0]]
print(matrix)
c=giveCoordinates(matrix)
print(c)


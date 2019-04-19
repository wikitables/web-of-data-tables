import numpy as np
import pandas as pd
import sys
#sys.path("/home/jhomara/torch/")
from PIL import Image
import matplotlib.pyplot as plt
import torch
import torch.nn as nn
from torch.autograd import Variable
import torch.utils.data as data

n_points = 20000
points = np.zeros((n_points,2))   # x, y
target = np.zeros((n_points,1))   # label
sigma = 0.5
for k in range(n_points):
    # Random selection of one class with 25% of probability per class.
    random = np.random.rand()
    if random<0.25:
        center = np.array([0,0])
        target[k,0] = 0   # This points are labeled 0.
    elif random<0.5:
        center = np.array([2,2])
        target[k,0] = 1   # This points are labeled 1.
    elif random<0.75:
        center = np.array([2,0])
        target[k,0] = 2   # This points are labeled 2.
    else:
        center = np.array([0,2])
        target[k,0] = 3   # This points are labeled 3.
    gaussian01_2d = np.random.randn(1,2)
    points[k,:] = center + sigma*gaussian01_2d

# Now, we write all the points in a file.
points_and_labels = np.concatenate((points,target),axis=1)   # 1st, 2nd, 3nd column --> x,y, label
pd.DataFrame(points_and_labels).to_csv('clas.csv',index=False)


# Here, we start properly the classifier.

# We read the dataset and create an iterable.
class my_points(data.Dataset):
    def __init__(self, filename):
        pd_data = pd.read_csv(filename, sep="\t", decimal=",").values  # Read data file.
        self.data = pd_data[:, 1:57]  # 1st and 2nd columns --> x,y
        self.target = pd_data[:, 57:]  # 3nd column --> label
        self.n_samples = self.data.shape[0]

    def __len__(self):  # Length of the dataset.
        return self.n_samples

    def __getitem__(self, index):  # Function that returns one point and one label.
        return torch.Tensor(self.data[index]), torch.Tensor(self.target[index])



# We create the dataloader.
my_data = my_points('/home/jhomara/Desktop/training.csv')
batch_size = 99
my_loader = data.DataLoader(my_data,batch_size=batch_size,num_workers=0)


# We build a simple model with the inputs and one output layer.
class my_model(nn.Module):
    def __init__(self, n_in=56, n_hidden=5, n_out=4):
        super(my_model, self).__init__()
        self.n_in = n_in
        self.n_out = n_out

        self.linearlinear = nn.Sequential(
            nn.Linear(self.n_in, self.n_out, bias=True),  # Hidden layer.
        )
        self.logprob = nn.LogSoftmax(dim=1)  # -Log(Softmax probability).

    def forward(self, x):
        x = self.linearlinear(x)
        x = self.logprob(x)
        return x

# Now, we create the mode, the loss function or criterium and the optimizer
# that we are going to use to minimize the loss.

# Model.
model = my_model()

# Negative log likelihood loss.
criterium = nn.NLLLoss()

# Adam optimizer with learning rate 0.1 and L2 regularization with weight 1e-4.
optimizer = torch.optim.Adam(model.parameters(),lr=0.1,weight_decay=1e-4)

# Taining.
#print(len(my_loader))
for k, (data, target) in enumerate(my_loader):
    print("k",k)
    # Definition of inputs as variables for the net.
    # requires_grad is set False because we do not need to compute the
    # derivative of the inputs.
    #print(target.shape)
    #print(data.shape)
    data = Variable(data, requires_grad=False)
    target = Variable(target.long(), requires_grad=False)

    # Set gradient to 0.
    optimizer.zero_grad()
    # Feed forward.
    pred = model(data)
    # Loss calculation.
    #print(target.view(-1))
    #print(pred)
    loss = criterium(pred, target.view(-1))
    # Gradient calculation.
    loss.backward()

    # Print loss every 10 iterations.
    #if k % 10 == 0:
    print('Loss {:.4f} at iter {:d}'.format(loss.item(), k))

    # Model weight modification based on the optimizer.
    optimizer.step()
#print(target.numpy())
pred = pred.exp().detach()     # exp of the log prob = probability.
_, index = torch.max(pred,1)   # index of the class with maximum probability.
print(np.transpose(index.numpy()))
# Now, we plot the results.
# Circles indicate the ground truth and the squares are the predictions.
"""
colors = ['r','b','g','y']
points = data.numpy()

# Ground truth.
target = target.numpy()
for k in range(1,4):
    select = target[:,0]==k
    p = points[select,:]
    plt.scatter(p[:,0],p[:,1],facecolors=colors[k])

# Predictions.
pred = pred.exp().detach()     # exp of the log prob = probability.
_, index = torch.max(pred,1)   # index of the class with maximum probability.
pred = pred.numpy()
index = index.numpy()
for k in range(1,4):
    select = index==k
    p = points[select,:]
    plt.scatter(p[:,0],p[:,1],s=60,marker='s',edgecolors=colors[k],facecolors='none')

plt.show()
"""
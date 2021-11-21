import fileinput
from math import pow
from math import sqrt

scores = []
for line in fileinput.input():
    scores.append(float(line.rstrip()))

mean = sum(scores) / len(scores)

variances = []
for score in scores:
    variances.append(pow((score-mean), 2))

stderr = sqrt(sum(variances)/len(variances))/sqrt(len(variances))

print("mean: {:.4f}, stderr: {:.4f}".format(mean, stderr))
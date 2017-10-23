import numpy as np
def geometric_mean(iterable):
    a = np.array(iterable)
    return a.prod()**(1.0/len(a))
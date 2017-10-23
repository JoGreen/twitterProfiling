import numpy as np
def geometric_mean(iterable):
    a = np.array(iterable)
    return (a* 1.0).prod()**(1.0/len(a))
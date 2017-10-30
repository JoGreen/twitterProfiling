import numpy as np
def geometric_mean(iterable):
    # type: (list[float])-> float
    if 0. in iterable: # no value = 0 ow product will be 0
        i = iterable.index(0.)
        iterable[i] = 1 * 10**(-16)

    a = np.array(iterable)

    return (a* 1.0).prod()**(1.0/len(a))



v = [2.2758620689655173, 2.7586206896551726, 2.4827586206896552, 0.896551724137931, 1.6551724137931034,
    2.413793103448276, 2.689655172413793, 2.5517241379310347, 3.310344827586207, 0.7586206896551724, 2.0,
    1.8620689655172413, 0.41379310344827586, 1.5862068965517242, 0.6206896551724138, 3.9310344827586206,
    3.103448275862069, 3.3793103448275863, 3.586206896551724, 4.344827586206897, 3.4482758620689653,
    2.0689655172413794, 1.6551724137931034, 0.896551724137931, 1.103448275862069, 1.0344827586206897,
    0.9655172413793104, 1.8620689655172413, 2.3448275862068964, 2.2758620689655173, 1.8620689655172413, 2.0,
    2.1379310344827585, 1.3103448275862069, 0.896551724137931, 1.6551724137931034]
print (geometric_mean(v) )
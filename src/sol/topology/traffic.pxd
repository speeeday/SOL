# coding=utf-8

import numpy as np
cimport numpy as np

cdef long _counter = 0
# noinspection PyClassicStyleClass
cdef class TrafficClass:
    cdef public int ID, priority, src, dst
    cdef public unicode name
    cdef public np.ndarray volFlows#, volBytes
    cdef public srcIPPrefix, dstIPPrefix


    cpdef tuple iepair(self)
    cpdef int ingress(self)
    cpdef int egress(self)
    cpdef volume(self, epoch=*)

cpdef make_tc(int src, int dst, volume, name=*)

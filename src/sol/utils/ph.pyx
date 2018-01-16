# coding=utf-8
"""
Various utility functions
"""
from collections import defaultdict

from six import u
from six.moves import zip
from sol.utils.logger import logger


cpdef unicode tup2str(tuple t):
    """ Convert tuple to string

    :param t: the tuple
    """
    return u'_'.join(map(u, (map(str, t))))

# Self-nesting dict
Tree = lambda: defaultdict(Tree)

def listeq(a, b):
    """
        Checks that two lists have equal elements
    """
    return len(a) == len(b) and all([x == y for x, y in zip(a, b)])

def uniq(alist):
    """
    Check that all elements are unique in a given list
    :param alist: list
    :return: true if all elements are different
    """
    return len(set(alist)) == len(alist)

def parse_bool(s):
    """ Parse a string into a boolean. Multiple truth values are supported,
    such as 'true', 'yes', 'y' and even 'ok' """
    return s.lower() in ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'ok', u'true']

def noop(x):
    """ Do nothing and return value that was passed in"""
    return x

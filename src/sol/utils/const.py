# coding=utf-8
"""
Contains library-wide strings and constants
"""
from enum import Enum, IntEnum


class Objective(Enum):
    """
    Supported pre-defined objectives
    """
    MIN_LINK_LOAD = u'minlinkload'
    MIN_NODE_LOAD = u'minnodeload'
    MIN_LATENCY = u'minlatency'
    MAX_FLOW = u'max_flow'
    MIN_ENABLED_NODES = u'min_enabled_nodes'


# Some useful objective constant
THE_OBJECTIVE = u'theobjective'
MIN_LOAD_PREFIX = 'MINLOAD'


class Constraint(Enum):
    """
    Supported pre-defined constraint templates
    """
    ROUTE_ALL = u'route_all'
    REQ_ALL_LINKS = u'req_all_links'
    REQ_ALL_NODES = u'req_all_nodes'
    REQ_SOME_LINKS = u'req_some_links'
    REQ_SOME_NODES = u'req_some_nodes'
    CAP_LINKS = u'cap_links'
    CAP_NODES = u'cap_nodes'
    FIX_PATHS = u'fix_path'
    MINDIFF = u'mindiff'
    NODE_BUDGET = u'node_budget'
    ALLOCATE_FLOW = u'allocate_flow'


class BinType(Enum):
    """
    Type of binary variable
    """
    BIN_NODE = u'node'
    BIN_EDGE = u'edge'
    BIN_LINK = BIN_EDGE
    BIN_PATH = u'path'


class ResConsumeMode(IntEnum):
    """
    Type of resource consumption. Currently supported modes are per flow or only once
    per path (e.g., TCAM/rule space constraints)
    """
    PER_FLOW = 1
    PER_PATH = 2


class NodeConsumeMode(IntEnum):
    """
    Resource consumption mode for nodes in a path.
    """
    ALL = 1  # All nodes with the resource
    MBOXES = 2  # Only middleboxes consume resources


class EpochComposition(Enum):
    """
    Modes for composing objective functions across epochs
    """
    AVG = u'sum'
    WORST = u'worst'


class Fairness(Enum):
    """
    Fairness modes for composition of objectives across different applications
    """
    WEIGHTED = u'weighted'
    UTILITARIAN = WEIGHTED
    PROPFAIR = u'propfair'
    MAXMIN = u'maxmin'
    NONE = u'none'

# A tolerance value for objective values
EPSILON = 1e-5

MAXSTR = u'max'
MEANSTR = u'mean'
MINSTR = u'min'
ALLSTR = u'all'
VALIDSTR = u'valid'

# CPLEX = u'cplex'
# GUROBI = u'gurobi'
# DEFAULT_OPTIMIZER = GUROBI
NODES = 'nodes'
LINKS = 'links'
PATHS = 'paths'
MBOXES = 'mboxes'

SELECT_RANDOM = u'random'
SELECT_SHORTEST = u'shortest'
SELECT_ANNEALING = u'sa'

# Pre-defined resource names
BANDWIDTH = u'bw'
CPU = u'cpu'
MEM = u'mem'
TCAM = u'tcam'
LATENCY = u'Latency'



# Topology fields
HAS_MBOX = u'hasMbox'
SWITCH = u'switch'
SERVICES = u'services'
RESOURCES = u'resources'

# Fields specific to FatTree topologies
CORE_LAYER = u'core'
EDGE_LAYER = u'edge'
AGG_LAYER = u'aggregation'

# Supported formats of topology files
FORMAT_GRAPHML = u'graphml'
FORMAT_GML = u'gml'
FORMAT_AUTO = u'auto'

# Error/warning strings
ERR_NO_GUROBI = u'Cannot use Gurobi Python API. Please install Gurobi and ' \
                u'gurobipy'
ERR_FMT = u'Given format is not supported'
ERR_NO_PATH = u'No paths between nodes {} and {}'
ERR_EPOCH_MISMATCH = u'Number of epochs insosistent across traffic classes'
ERR_BAD_CAPVAL = u'Bad resource cap value. Must be between 0 and 1'
ERR_NO_RESOURCE = u'Node/link %s has no resource %s. Cannot add a cap to non-existing resource'
ERR_UNKNOWN_MODE = u'Uknown %s mode: %s'
ERR_UNKNOWN_TYPE = u'Uknonw %s type: %s'
ERR_ODD_ARITY = u'-arity of a FatTree topology must be even'
ERR_OP_NOT_SUPP = u'Operation not supported'
ERR_NO_NORM = "Not normalizing objective functions can produce invalid results, "\
              "especially when composing applications"
# WARN_NO_PATH_ID = u'No ID given to Path constructor, ' \
#                   u'generating a random path ID'

import argparse
import logging

import copy
import sys
import networkx as nx
import flask
import numpy
from attrdict import AttrDict
from flask import Flask, request, jsonify
from flask import abort
from flask import send_from_directory
from flask_compress import Compress
from sol.opt import NetworkConfig, NetworkCaps
from sol.opt.composer import compose_apps
from sol.path.generate import generate_paths_ie, use_mbox_modifier
from sol.path.paths import PPTC
from sol.path.predicates import null_predicate, has_mbox_predicate
from sol.topology.topologynx import Topology
from sol.topology.traffic import TrafficClass
from sol.utils.const import EpochComposition, Fairness, Constraint, Objective, NODES, LINKS, ERR_UNKNOWN_MODE, MBOXES, HAS_MBOX

from sol.opt.app import App

app = Flask(__name__)
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())

# REST-specific configuration here
__API_VERSION = 1  # the current api version
_json_pretty = False  # whether to pretty print json or not (not == save space)
_gzip = True  # whether to gzip the returned responses


# SET THIS TO THE TOPOLOGY WE ARE TESTING
_topology = None

_predicatedict = {
    'null': null_predicate,
    'null_predicate': null_predicate,
    'has_mbox' : has_mbox_predicate, 
    'has_mbox_predicate' : has_mbox_predicate, 
    'has_middlebox' : has_mbox_predicate, 
    'has_middlebox_predicate' : has_mbox_predicate 
}

def json_to_topology(data):
    _relabels = {}
    topology = Topology.from_json(data)
    logging.info('Topology read successfully as:')
    logging.info(topology.to_json())
    logging.info('\n')
    
    # compute relabels
    # TODO: find bug in relabels!!! otherwise use simple topology for demo
    print "starting relbaels:"
    graph = topology.get_graph()
    while len(_relabels) < topology.num_nodes():
        for (src,dst) in topology.links():
            srcname = int(graph[src][dst][u'srcname'])
            dstname = int(graph[src][dst][u'dstname'])
            if src not in _relabels:
                _relabels[src] = srcname
            if dst not in _relabels:
                _relabels[dst] = dstname
        print "finished links with size: " + str(len(_relabels))
    topology.set_graph(nx.relabel_nodes(graph, _relabels))
        
    logging.info('Relabeled Topology as:')
    logging.info(topology.to_json())
    logging.info('\n')

    return topology


def assign_to_tc(tcs, paths, name):
    """
    Assign paths to traffic classes based on the ingress & egress nodes.
    To be used only if there is one traffic class (predicate)
    per ingress-egress pair.

    :param tcs: list of traffic classes
    :param paths: dictionary of paths: ..

            paths[ingress][egress] = list_of_paths

    :return: paths per traffic class
    """
    pptc = PPTC()
    for tc in tcs:
        pptc.add(name, tc, paths[tc.ingress()][tc.egress()])
    return pptc


@app.route('/')
@app.route('/api/v1/hi')
def hi():
    """
    A simple greeting to ensure the server is up

    """
    return u"Hello, this is SOL API version {}".format(__API_VERSION)

def toConstraint(con):
    if con == u'route_all':
        return [Constraint.ROUTE_ALL, [], {}]
    elif con == u'allocate_flow':
        return [Constraint.ALLOCATE_FLOW, [], {}] #CHECK IF THIS CASE IS RIGHT
    elif con == u'req_all_links':
        return [Constraint.REQ_ALL_LINKS, [], {}]
    elif con == u'req_all_nodes':
        return [Constraint.REQ_ALL_NODES, [], {}]
    elif con == u'req_some_links':
        return [Constraint.REQ_SOME_LINKS, [], {}]
    elif con == u'req_some_nodes':
        return [Constraint.REQ_SOME_NODES, [], {}]
    elif con == u'cap_links':
        return [Constraint.CAP_LINKS, [], {}]
    elif con == u'cap_nodes':
        return [Constraint.CAP_NODES, [], {}]
    elif con == u'fix_path':
        return [Constraint.FIX_PATHS, [], {}]
    elif con == u'mindiff':
        return [Constraint.MINDIFF, [], {}]
    elif con == u'node_budget':
        return [Constraint.NODE_BUDGET, [], {}]
    else:
        logger.debug("Received Unknown Constraint for Map: " + str(con))
        return None
    
@app.route('/api/v1/compose', methods=['POST'])
def composeview():
    """
    Create a new composed opimization, solve and return the

    :return:

    """
    try:
        data = request.get_json()
        logger.debug('\nReceived JSON:')
        logger.debug(data)
        logger.debug('\n')

        apps_json = data['apps']

        topology = json_to_topology(data['topology'])
        graph = topology.get_graph()

        for n in range(len(data['topology']['nodes'])):
            if (u'hasMbox' in data['topology']['nodes'][n]):
                if (data['topology']['nodes'][n][u'hasMbox'] == u'true'):
                    topology.set_mbox(data['topology']['nodes'][n]['id'])
                    del graph.node[n][u'hasMbox']
                    topology.set_graph(graph)
        logger.debug('Set the Middleboxes: ')
        logger.debug(topology.to_json())
        logger.debug('\n')
        
    except KeyError:
        abort(400)

    apps = []
    curr_paths = copy.deepcopy(_paths)

    logger.debug('Paths in Topology:')
    logger.debug(curr_paths)
    logger.debug('\n')
    
    for aj in apps_json:
        
        aj = AttrDict(aj)   
        curr_predicate = _predicatedict[aj.predicate]

        logger.debug('Taking the Following Predicate into Account:')
        logger.debug(aj.predicate)
        logger.debug('\n')


        for src in _paths:
            logger.debug('Looking at Node ' + str(src))
            src_paths = _paths[src]
            for dst in src_paths:
                logger.debug('\tPaths for Node ' + str(dst))
                paths = src_paths[dst]
                for p in paths:
                    logger.debug('\t\tLooking at Path: ' + str(p))
                    if not curr_predicate(p, topology):
                        logger.debug('\t\t\tRemoving Path Under Predicate: ' + str(p))
                        curr_paths[src][dst].remove(p)
                if (len(curr_paths[src][dst]) == 0):
                    logger.debug('\n\t\t\tCOULD NOT FIND VALID PATH BETWEEN THE PAIR OF NODES UNDER THE PREDICATE: (' + str(src) + ',' + str(dst) + ')\n')
        logger.debug('\n')
                        
    logger.debug('Updated Paths for Optimization: ')
    logger.debug(curr_paths)
    logger.debug('\n')
    
    for aj in apps_json:
        
        aj = AttrDict(aj)

        logger.debug(aj.id + ': ')
        
        tcs = []
        for tcj in aj.traffic_classes:
            tc = TrafficClass(tcj.tcid, u'tc', tcj.src, tcj.dst,
                              numpy.array([tcj.vol_flows]))
            tcs.append(tc)
            
        pptc = assign_to_tc(tcs, curr_paths, aj.id)
        # pptc = generate_paths_tc(topology, tcs, _predicatedict[aj.predicate], 20,
        #                          float('inf'), name=aj.id)
        # objective
        if aj.objective.get('resource') is None:
            objective = (aj.objective.name)
        else:
            objective = (aj.objective.name, aj.objective.resource)

        logger.debug("Objective: " + str(objective))
            
        for r in map(AttrDict, aj.resource_costs):
            logger.debug("Resource Type: " + str(r.resource))
        # [TODO: NEED TO UNDERSTAND THE DIFFERENCE BETWEEN THE MODES (NODES/MBOXES/LINKS)]
        # resource : (mode, cost_value, cost_function)
        resource_cost = {r.resource: (LINKS, r.cost, 0) for r in map(AttrDict, aj.resource_costs)}
        logger.debug("Constraints: " + str(list(aj.constraints)))
        wrap_constraints = []
        for con in list(aj.constraints):
            if con == u'route_all':
                wrap_constraints.append([Constraint.ROUTE_ALL, [], {}])
            elif con == u'allocate_flow':
                do_nothing = 1 # do nothing here because allocate_flow is called when opt is initialized
            elif con == u'req_all_links':
                wrap_constraints.append([Constraint.REQ_ALL_LINKS, [], {}])
            elif con == u'req_all_nodes':
                wrap_constraints.append([Constraint.REQ_ALL_NODES, [], {}])
            elif con == u'req_some_links':
                wrap_constraints.append([Constraint.REQ_SOME_LINKS, [], {}])
            elif con == u'req_some_nodes':
                wrap_constraints.append([Constraint.REQ_SOME_NODES, [], {}])
            elif con == u'cap_links':
                wrap_constraints.append([Constraint.CAP_LINKS, [], {}])
            elif con == u'cap_nodes':
                wrap_constraints.append([Constraint.CAP_NODES, [], {}])
            elif con == u'fix_path':
                wrap_constraints.append([Constraint.FIX_PATHS, [], {}])
            elif con == u'mindiff':
                wrap_constraints.append([Constraint.MINDIFF, [], {}])
            elif con == u'node_budget':
                wrap_constraints.append([Constraint.NODE_BUDGET, [], {}])
            else:
                logger.debug("Received Unknown Constraint for Map: " + str(con))

        wrap_objectives = []
        if objective[0] == u'minlinkload':
            wrap_objectives.append(Objective.MIN_LINK_LOAD)
        elif objective[0] == u'minnodeload':
            wrap_objectives.append(Objective.MIN_NODE_LOAD)
        elif objective[0] == u'minlatency':
            wrap_objectives.append(Objective.MIN_LATENCY)
        elif objective[0] == u'maxflow':
            wrap_objectives.append(Objective.MAX_FLOW)
        elif objective[0] == u'minenablednodes':
            wrap_objectives.append(Objective.MIN_ENABLED_NODES)
        else:
            logger.debug("Couldn't find Objective Name")
            obj_name = None
        wrap_objectives.append([objective[1]]) #*args
        wrap_objectives.append({}) #**kwargs

        logger.debug('\n')
        apps.append(App(pptc, wrap_constraints, resource_cost, wrap_objectives, name=aj.id))

    ncaps = NetworkCaps(topology)
    for r in resource_cost.keys():
        ncaps.add_cap(r,None,1)
    opt = compose_apps(apps, topology, NetworkConfig(networkcaps=ncaps), epoch_mode=EpochComposition.WORST, fairness=Fairness.WEIGHTED, weights = None)
    opt.solve()
    result = []
    for app in apps:
        result_app = {"app": app.name, "tcs": []}
        result_pptc = opt.get_paths(0)
        it = (app.pptc).tcs()
        while True:
            try:
                tc = it.next()
                obj = {
                    "tcid": tc.ID,
                    "paths": []
                }
                for p in result_pptc.paths(tc):
                    if p.flow_fraction() != 0:
                        obj["paths"].append({
                            "nodes": p.nodes().tolist(),
                            "fraction": p.flow_fraction()
                        })
                result_app["tcs"].append(obj)
            except StopIteration:
                break
        result.append(result_app)

    logger.debug('\nComputed App Composition: ')
    logger.debug(result)
    logger.debug('\n')
    
    return jsonify(result)


@app.route('/api/v1/topology/', methods=['GET', 'POST'])
def topology():
    """
    Set or return the stored topology

    """

    #TODO: make sure the mbox nodes are identified for the topology object

    logger.debug("\nSetting Global Variables Topology and Paths\n")
    global _topology, _paths
    if request.method == 'GET':
        if _topology is None:
            return
        return jsonify(_topology.to_json())
    elif request.method == 'POST':
        data = request.get_json()
        logger.debug('Received JSON:')
        logger.debug(data)
        logger.debug('\n')
        _topology = json_to_topology(data)
#        print _topology.to_json()
        _paths = {}
        for s in _topology.nodes():
            _paths[s] = {}
            for t in _topology.nodes():
                if s != t:
                    _paths[s][t] = list(generate_paths_ie(s, t, _topology, null_predicate, 100, len(list(_topology.nodes()))))

        logging.debug("Computed Paths:")
        logging.debug(str(_paths))
        logging.debug('\n')
        return ""
    else:
        # method not allowed
        abort(405)


@app.route('/apidocs/')
def docs(path='index.html'):
    """
    Endpoint for swagger UI
    :param path:
    :return:
    """
    return send_from_directory('static/swaggerui/', path)


@app.route('/spec')
def swagger_json():
    """
    Endpoint that returns the swagger api using JSON
    :return:
    """
    with open('static/api.json', 'r') as f:
        return jsonify(flask.json.loads(f.read()))
        # return url_for('static', filename='api.json')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev', action='store_true')
    parser.add_argument('--debug', action='store_true')
    options = parser.parse_args()

    if options.dev:
        _json_pretty = True
        _gzip = False

    if _gzip:
        c = Compress()
        c.init_app(app)
    if options.debug:
        logger.setLevel(logging.DEBUG)
    app.run(debug=options.dev)

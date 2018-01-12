# coding=utf-8
# cython: profile=True
from __future__ import division
from __future__ import print_function

from numpy import array, stack
from sol.opt.gurobiwrapper import OptimizationGurobi
from sol.opt.gurobiwrapper cimport OptimizationGurobi
from sol.topology.topologynx cimport Topology
from sol.path.paths import PPTC
from sol.path.paths cimport PPTC

from sol.utils.const import EpochComposition, Fairness, Objective, NODES, LINKS, ERR_UNKNOWN_MODE, MBOXES
from sol.utils.exceptions import InvalidConfigException
from sol.utils.logger import logger


cpdef compose_apps(apps, Topology topo, network_config, epoch_mode=EpochComposition.AVG, fairness=Fairness.WEIGHTED,
                   weights=None):
    """
    Compose multiple applications into a single optimization
    :param apps: a list of App objects
    :param topo: Topology
    :param network_config: Network configuration (contains resource caps)
    :param epoch_mode: how is the objective computed across different epochs.
        Default is the maximum obj function across epochs. See :py:class:`~sol.EpochComposition`
    :param fairness: type of objective composition. See :py:class:`~sol.ComposeMode`
    :param weights: only applies if fairness is WEIGHTED. Higher weight means higher priority
        (only relative to each other). That is if apps have weights 0.5 and 1, app with priority 1
        is given more importance. Setting weights to be equal (both either 0.5 or 1) has no effect on
        fairness
    :return:
    """
    # TODO: refactor epoch_mode and fairness into network config?
    logger.debug("Starting composition")

    # Merge all paths per traffic class into a single object so we can start the optimization
    all_pptc = PPTC.merge([a.pptc for a in apps])

    # Start the optimization
    opt = OptimizationGurobi(topo, all_pptc)
    # Extract the capacities from all links and nodes
    node_caps = {node: topo.get_resources(node) for node in topo.nodes()}
    link_caps = {link: topo.get_resources(link) for link in topo.links()}

    # Consume network resources. For each resource, generate resource constraints by considering the
    # load imposed by all traffic classes
    rset = set()
    for app in apps:
        rset.update(app.resource_cost.keys())
    # print (rset)
    for r in rset:
        logger.debug("App Resource Cost List: " + str(r))
        logger.debug(str([app.resource_cost[r] for app in apps if r in app.resource_cost]))
        modes, cost_vals, cost_funcs = zip(*[app.resource_cost[r] for app in apps if r in app.resource_cost])
        # Make sure all the modes agree for a given resource
        assert len(set(modes)) == 1
        mode = modes[0]
        if mode == NODES or mode== MBOXES:
            capacities = {n: node_caps[n][r] for n in node_caps if r in node_caps[n]}
        elif mode == LINKS:
            capacities = {l: link_caps[l][r] for l in link_caps if r in link_caps[l]}
        else:
            raise InvalidConfigException(ERR_UNKNOWN_MODE % ('resource owner', mode))
        # Avoid using cost funcs for now
        # TODO: figure out efficient way of evaluating cost funcs
        opt.consume(all_pptc.tcs(), r, capacities, mode, max(cost_vals), None)

    # Cap the resources, if caps were given
    if network_config is not None:
        caps = network_config.get_caps()
        if caps is not None:
            for r in caps.resources():
                opt.cap(r, caps.caps(r))

    # And add any other constraints the app might desire
    for app in apps:
        opt.add_named_constraints(app)

    # Compute app weights
    if weights is None:
        volumes = stack([app.epoch_volumes() for app in apps], axis=0)
        weights = volumes / volumes.sum(axis=0)
    else:
        assert 0 < weights <= 1

    logger.debug('App weights %s' % weights)

    # Add objectives
    objs = []
    for app in apps:
        logger.debug("Currently on app: " + str(app))
        logger.debug("App Objectives: " + str(app.obj))

        kwargs = app.obj[2].copy()    #THIS COPY CALL KEEPS SEGFAULTING
        logger.debug("Just copied kwargs")
        kwargs.update(dict(varname=app.name, tcs=app.obj_tc))
        logger.debug("Just finished update")            
        epoch_objs = opt.add_single_objective(app.obj[0], *app.obj[1], **kwargs)
        logger.debug("Added Objective")
        objs.append(epoch_objs)


    logger.debug("Composing Objectives")
        
    opt.compose_objectives(array(objs), epoch_mode, fairness, weights)
    return opt


###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2011, Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 or (at your
# option) any later version as published by the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################

import logging
LOG = logging.getLogger('zen.RabbitMQ')

import re
import json
import urllib

from itertools import chain

from Products.DataCollector.plugins.CollectorPlugin import PythonPlugin
from Products.DataCollector.plugins.DataMaps import ObjectMap, RelationshipMap
from Products.ZenUtils.Utils import prepId


class RabbitMQ(PythonPlugin):

    def collect(self, device, log):
        # FIXME - This should use a zProp
        rabbitmq_url = "http://guest:guest@%s:55672" % device.manageIp

        foo = {}
        types = ['nodes', 'vhosts', 'exchanges', 'queues']

        for i in types:
            try:
                url = "%s/api/%s" % (rabbitmq_url, i)
                f = urllib.urlopen(url)
                foo[i] = f.read()
            except:
                LOG.info('unable to access rabbit')


        return foo


    def process(self, device, results, unused):
        LOG.info('Trying RabbitMQ on %s', device.id)

        maps = []

        # nodes - only one for now
        node_title = None
        node_id = None
        nodes = []

        nodejson = json.loads(results['nodes'])

        for item in nodejson:
            if item['name']:
                node_title = item['name']
                node_id = prepId(node_title)
                nodes.append(ObjectMap(data={
                    'id': node_id,
                    'title': node_title,
                    }))

                continue

        if len(nodes) > 0:
            LOG.info('Found node %s on %s', node_title, device.id)
        else:
            LOG.info('No node found on %s', device.id)
            return None

        maps.append(RelationshipMap(
            relname='rabbitmq_nodes',
            modname='ZenPacks.community.RabbitMQ.RabbitMQNode',
            objmaps=nodes))

        # vhosts
        maps.extend(self.getVHostRelMap(
            device, results, 'rabbitmq_nodes/%s' % node_id))

        return maps

    def getVHostRelMap(self, device, results, compname):
        rel_maps = []
        object_maps = []

        vhostjson = json.loads(results['vhosts'])

        for item in vhostjson:
            if item['name']:
                vhost_title = item['name']
                vhost_id = prepId(vhost_title)

                object_maps.append(ObjectMap(data={
                    'id': vhost_id,
                    'title': vhost_title,
                    }))

                exchanges = self.getExchangeRelMap(results['exchanges'], vhost_title,
                    '%s/rabbitmq_vhosts/%s' % (compname, vhost_id))

                queues = self.getQueueRelMap(results['queues'], vhost_title,
                    '%s/rabbitmq_vhosts/%s' % (compname, vhost_id))

                LOG.info(
                    'Found vhost %s with %d exchanges and %d queues on %s',
                    vhost_title, len(exchanges.maps), len(queues.maps),
                    device.id)

                rel_maps.append(exchanges)
                rel_maps.append(queues)

        return [RelationshipMap(
            compname=compname,
            relname='rabbitmq_vhosts',
            modname='ZenPacks.community.RabbitMQ.RabbitMQVHost',
            objmaps=object_maps)] + rel_maps



    def getExchangeRelMap(self, vhost, exchanges, compname):
        object_maps = []

        for item in exchanges:
            if not item['vhost'] == vhost:
                continue

            if not item['name']:
                item['name'] = 'amq.default'

            object_maps.append(ObjectMap(data={
                'id': prepId(item['name']),
                'title': item['name'],
                'exchange_type': item['type'],
                'durable': item['durable'],
                'auto_delete': item['auto_delete'],
                'arguments': ','.join(chain(*item['arguments'].items())),
                }))


        return RelationshipMap(
            compname=compname,
            relname='rabbitmq_exchanges',
            modname='ZenPacks.community.RabbitMQ.RabbitMQExchange',
            objmaps=object_maps)

    def getQueueRelMap(self, vhost, queues, compname):
        object_maps = []

        for item in queues:
            if not item['vhost'] == vhost:
                continue

            object_maps.append(ObjectMap(data={
                'id': prepId(item['name']),
                'title': item['name'],
                'durable': item['durable'],
                'auto_delete': item['auto_delete'],
                'arguments': ','.join(chain(*item['arguments'].items())),
                }))

        return RelationshipMap(
            compname=compname,
            relname='rabbitmq_queues',
            modname='ZenPacks.community.RabbitMQ.RabbitMQQueue',
            objmaps=object_maps)

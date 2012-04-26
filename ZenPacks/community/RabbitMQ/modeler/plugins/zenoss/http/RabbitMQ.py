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

                exchanges = self.getExchangeRelMap(results['exchanges'],
                    '%s/rabbitmq_vhosts/%s' % (compname, vhost_id))

                queues = self.getQueueRelMap(results['queues'],
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



    def getExchangeRelMap(self, exchanges_string, compname):
        object_maps = []
        for exchange_string in exchanges_string.split('\n'):
            if not exchange_string.strip():
                continue

            name, exchange_type, durable, auto_delete, arguments = \
                re.split(r'\s+', exchange_string)

            if not name:
                name = 'amq.default'

            if re.search(r'true', durable, re.I):
                durable = True
            else:
                durable = False

            if re.search(r'true', auto_delete, re.I):
                auto_delete = True
            else:
                auto_delete = False

            object_maps.append(ObjectMap(data={
                'id': prepId(name),
                'title': name,
                'exchange_type': exchange_type,
                'durable': durable,
                'auto_delete': auto_delete,
                'arguments': arguments,
                }))

        return RelationshipMap(
            compname=compname,
            relname='rabbitmq_exchanges',
            modname='ZenPacks.community.RabbitMQ.RabbitMQExchange',
            objmaps=object_maps)

    def getQueueRelMap(self, queues_string, compname):
        object_maps = []
        for queue_string in queues_string.split('\n'):
            if not queue_string.strip():
                continue

            name, durable, auto_delete, arguments = \
                re.split(r'\s+', queue_string)

            if re.search(r'true', durable, re.I):
                durable = True
            else:
                durable = False

            if re.search(r'true', auto_delete, re.I):
                auto_delete = True
            else:
                auto_delete = False

            object_maps.append(ObjectMap(data={
                'id': prepId(name),
                'title': name,
                'durable': durable,
                'auto_delete': auto_delete,
                'arguments': arguments,
                }))

        return RelationshipMap(
            compname=compname,
            relname='rabbitmq_queues',
            modname='ZenPacks.community.RabbitMQ.RabbitMQQueue',
            objmaps=object_maps)

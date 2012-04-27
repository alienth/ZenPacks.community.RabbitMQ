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

import json
import md5
import os
import re
import tempfile
import time

from Products.ZenRRD.CommandParser import CommandParser


def getTempFilename(keys):
    return os.path.join(
        tempfile.gettempdir(),
        '.zenoss_rabbitmq_%s' % md5.md5('+'.join(keys)).hexdigest())


def saveData(keys, data):
    tmpfile = getTempFilename(keys)
    tmp = open(tmpfile, 'w')
    json.dump(data, tmp)
    tmp.close()


def loadData(keys, expiration=1800):
    tmpfile = getTempFilename(keys)
    if not os.path.isfile(tmpfile):
        return None

    # Make sure temporary data isn't too stale.
    if os.stat(tmpfile).st_mtime < (time.time() - expiration):
        os.unlink(tmpfile)
        return None

    tmp = open(tmpfile, 'r')
    data = json.load(tmp)
    tmp.close()

    return data


class RabbitMQAPI(CommandParser):
    eventKey = eventClassKey = 'rabbitmq_node_status'

    event = None

    def processResults(self, cmd, result):
        """
        Router method that allows this parser to be used for all rabbitmqctl
        subcommands.
        """

        # Get as much error handling out of the way right away.
        if self.isError(cmd, result):
            return

        # Route to the right parser based on the command.
        if '/api/overview' in cmd.command:
            self.processStatusResults(cmd, result)
        elif '/api/aliveness-test' in cmd.command:
            self.processAliveResults(cmd, result)
        elif '/api/connections' in cmd.command:
            self.processListConnectionsResults(cmd, result)
        elif '/api/channels' in cmd.command:
            self.processListChannelsResults(cmd, result)
        elif '/api/queues' in cmd.command:
            self.processListQueuesResults(cmd, result)

    def processStatusResults(self, cmd, result):
        result.events.append(self.getEvent(
            cmd, "node status is OK", clear=True))

    def processAliveResults(self, cmd, result):

        data = json.loads(result)
        if data['status'] == 'ok':
            result.events.append(self.getEvent(
                cmd, "alive check OK", clear=True))
        else:
            result.events.append(self.getEvent(
                cmd, "alive check failed"))

    def processListConnectionsResults(self, cmd, result):
        connections = json.loads(result)

        dp_map = dict([(dp.id, dp) for dp in cmd.points])

        gauge_metrics = ('connections', 'channels', 'sendQueue')
        #delta_metrics = ('recvBytes', 'recvCount', 'sendBytes', 'sendCount')

        # Rather than not record data when no connections are open we need to
        # records zeros.
        if len(connections) < 1:
            for metric in gauge_metrics
                if metric in dp_map:
                    result.values.append((dp_map[metric], 0))

            return

        # Metrics that don't require getting a difference since the last
        # collection.
        if 'connections' in dp_map:
            result.values.append((
                dp_map['connections'], len(connections)))

        if 'channels' in dp_map:
            result.values.append((dp_map['channels'], reduce(
                lambda x, y: x + y,
                (x['channels'] for x in connections))))

        if 'sendQueue' in dp_map:
            result.values.append((dp_map['sendQueue'], reduce(
                lambda x, y: x + y,
                (x['sendQueue'] for x in connections))))


    def processListChannelsResults(self, cmd, result):
        channels = {}

        for line in cmd.result.output.split('\n'):
            if not line:
                continue

            fields = re.split(r'\s+', line.rstrip())

            # pid consumer_count messages_unacknowledged acks_uncommitted
            if len(fields) != 4:
                return

            channels[fields[0]] = dict(
                consumers=int(fields[1]),
                unacknowledged=int(fields[2]),
                uncommitted=int(fields[3]),
                )

        dp_map = dict([(dp.id, dp) for dp in cmd.points])

        metrics = ('consumers', 'unacknowledged', 'uncommitted')

        # Rather than not record data when no connections are open we need to
        # records zeros.
        if len(channels.keys()) < 1:
            for metric in metrics:
                if metric in dp_map:
                    result.values.append((dp_map[metric], 0))

            return

        for metric in metrics:
            if metric in dp_map:
                result.values.append((dp_map[metric], reduce(
                    lambda x, y: x + y,
                    (x[metric] for x in channels.values()))))

    def processListQueuesResults(self, cmd, result):
        queues = {}

        for line in cmd.result.output.split('\n'):
            if not line:
                continue

            fields = re.split(r'\s+', line.rstrip())

            # name messages_ready messages_unacknowledged messages consumers
            # memory
            if len(fields) != 6:
                return

            queues[fields[0]] = dict(
                ready=int(fields[1]),
                unacknowledged=int(fields[2]),
                messages=int(fields[3]),
                consumers=int(fields[4]),
                memory=int(fields[5]),
                )

        if len(queues.keys()) < 1:
            return

        metrics = (
            'ready', 'unacknowledged', 'messages', 'consumers', 'memory',
            )

        for point in cmd.points:
            if point.component in queues and point.id in metrics:
                result.values.append((
                    point, queues[point.component][point.id]))

    def isError(self, cmd, result):
        try:
            json.loads(result)
        except:
            result.events.append(self.getEvent(
                cmd, "could not parse json"))
            return True

        if cmd.result.exitCode != 0:
            result.events.append(self.getEvent(
                cmd, "error reading rabbitmq api",
                message=cmd.result.output))

            return True

        return False

    def getEvent(self, cmd, summary, message=None, clear=False):
        event = dict(
            summary=summary,
            component=cmd.component,
            eventKey=self.eventKey,
            eventClassKey=self.eventClassKey,
            )

        if message:
            event['message'] = message

        if clear:
            event['severity'] = 0
        else:
            event['severity'] = cmd.severity

        return event

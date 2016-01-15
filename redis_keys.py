# redis-collectd-plugin - redis_keys.py
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; only version 2 of the License is applicable.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
#
# Authors:
#   Garret Heaton <powdahound at gmail.com>
# Contributors:
#   Pierre Mavro <p.mavro@criteo.com> / Deimosfr <deimos at deimos.fr>
#   https://github.com/powdahound/redis-collectd-plugin/graphs/contributors
#
# About this plugin:
#   This plugin uses collectd's Python plugin to record Redis information.
#
# collectd:
#   http://collectd.org
# Redis:
#   http://redis.googlecode.com
# collectd-python:
#   http://collectd.org/documentation/manpages/collectd-python.5.shtml

import collectd
import redis
import redis.exceptions
import re

# Verbose logging on/off. Override in config by specifying 'Verbose'.
VERBOSE_LOGGING = False

CONFIGS = []
REDIS_INFO = {}
KEY_INFO = {}

PREFIX = 'redis_keys plugin'

def configure_callback(conf):
    """Receive configuration block"""
    host = None
    port = None
    auth = None
    instance = None
    missing_key_value = 0

    redis_pat = re.compile(r'redis_(.*)$', re.M|re.I)
    key_pat = re.compile(r'key_(.*)$', re.M|re.I)
    for node in conf.children:
        key = node.key.lower()
        val = node.values[0]
        log_verbose('Analyzing config %s key (value: %s)' % (key, val))
        search_redis = redis_pat.search(key)
        search_key = key_pat.search(key)

        if key == 'host':
            host = val
        elif key == 'port':
            port = int(val)
        elif key == 'auth':
            auth = val
        elif key == 'verbose':
            global VERBOSE_LOGGING
            VERBOSE_LOGGING = bool(node.values[0]) or VERBOSE_LOGGING
        elif key == 'instance':
            instance = val
        elif key == 'missing_key_value':
            missing_key_value = int(val)
        elif search_redis:
            log_verbose('Matching redis expression found: key: %s - value: %s' % (search_redis.group(1), val))
            global REDIS_INFO
            REDIS_INFO[search_redis.group(1)] = val
        elif search_key:
            log_verbose('Matching key expression found: key: %s - value: %s' % (search_key.group(1), val))
            global KEY_INFO
            KEY_INFO[search_key.group(1)] = val
        else:
            collectd.warning('%s: Unknown config key: %s.' % (PREFIX, key) )
            continue

    log_verbose('Configured with host=%s, port=%s, instance name=%s, using_auth=%s' % ( host, port, instance, auth!=None))

    CONFIGS.append( { 'host': host, 'port': port, 'auth':auth, 'instance':instance, 'missing_key_value' : missing_key_value } )

def dispatch_value(info, key, type, plugin_instance=None, type_instance=None):
    """Read a key from info response data and dispatch a value"""
    if key not in info:
        collectd.warning('%s: Info key not found: %s' % (PREFIX,key))
        return

    if plugin_instance is None:
        plugin_instance = 'unknown redis'
        collectd.error('%s: plugin_instance is not set, Info key: %s' % (PREFIX,key))

    if not type_instance:
        type_instance = key

    try:
        value = int(info[key])
    except ValueError:
        value = float(info[key])

    log_verbose('Sending value: %s=%s' % (type_instance, value))

    val = collectd.Values(plugin='redis_keys')
    val.type = type
    val.type_instance = type_instance
    val.plugin_instance = plugin_instance
    val.values = [value]
    val.dispatch()

def read_callback():
    for conf in CONFIGS:
        get_metrics( conf )

def get_metrics( conf ):
    info = None
    r = redis.StrictRedis(host=conf['host'],port=conf['port'], password=conf['auth'])
    try:
        info = r.info()
    except redis.exceptions.ConnectionError,e:
        collectd.error('%s: Error connecting to %s:%d - %r'
                       % (PREFIX, conf['host'], conf['port'], e))

    if not info:
        collectd.error('%s: No info received' % PREFIX)
        return

    plugin_instance = conf['instance']
    if plugin_instance is None:
        plugin_instance = '{host}:{port}'.format(host=conf['host'], port=conf['port'])

    for key, val in REDIS_INFO.iteritems():
        #log_verbose('key: %s - value: %s' % (key, val))
        if key == 'total_connections_received':
            dispatch_value(info, 'total_connections_received', 'counter', plugin_instance, 'connections_received')
        elif key == 'total_commands_processed':
            dispatch_value(info, 'total_commands_processed', 'counter', plugin_instance, 'commands_processed')
        else:
            dispatch_value(info, key, val, plugin_instance)

    for key, val in KEY_INFO.items():
        metric,_ = key_metric(r, key)
        if metric is None:
            metric = conf['missing_key_value']
        dispatch_value({ key : metric}, key, val, plugin_instance)


def key_metric(r, key):
    keytype = r.type(key)
    get_metric = {
        'list'  :lambda r,k: r.llen(k),
        'hash'  :lambda r,k: r.hlen(k),
        'set'   :lambda r,k: r.scard(k),
        'zset'  :lambda r,k: r.zcard(k),
        'string':lambda r,k: r.get(k),
        'none'  :lambda r,k: None
    }
    return get_metric[keytype](r,key) , keytype


def log_verbose(msg):
    if not VERBOSE_LOGGING:
        return
    collectd.info('%s [verbose]: %s' % (PREFIX,msg))


# register callbacks
collectd.register_config(configure_callback)
collectd.register_read(read_callback)

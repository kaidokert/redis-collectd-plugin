import unittest

import redis_keys


class data:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def conf(name, val=None):
    if not type(val) is list: val = [val]
    return data(key=name, values=val)


def mkconf(port=6379, inst='normal', key='redis_uptime_in_days'):
    return data(children=[
        conf('verbose', True),
        conf('host', 'localhost'),
        conf('instance', inst),
        conf('port', port),
        conf(key)
    ])


class TestRedisKeys(unittest.TestCase):
    def setUp(self):
        class test_hook(object):
            collector = self

            def dispatch(self):
                vl = {self.type_instance: self.values}
                vs = self.collector.disp_values
                name = self.plugin_instance
                if name in vs:
                    vs[name].update(vl)
                else:
                    vs[name] = vl

        self.disp_values = {}
        redis_keys.stand_in = test_hook
        self.r = redis_keys
        self.r.CONFIGS = []
        self.r.REDIS_INFO = {}
        self.r.KEY_INFO = {}
        self.r.VERBOSE_LOGGING = True

    def test_bad_setup(self):
        self.r.configure_callback(data(children=[
            conf('foo', 'bar'),
        ]))
        self.r.read_callback()

    def test_config_one(self):
        self.r.configure_callback(mkconf())
        self.r.read_callback()
        self.r.read_callback()
        v = self.disp_values
        self.assertIn('normal', v)

    def test_config_two_broken(self):
        self.r.configure_callback(mkconf())
        self.r.configure_callback(mkconf(6378, 'x-files', 'redis_used_memory'))
        self.r.read_callback()
        self.r.read_callback()
        v = self.disp_values
        self.assertIn('normal', v)
        self.assertNotIn('x-files', v)

    def test_config_two_ok(self):
        self.r.configure_callback(mkconf())
        self.r.configure_callback(mkconf(6379, 'x-files', 'redis_used_memory'))
        self.r.read_callback()
        self.r.read_callback()
        v = self.disp_values
        self.assertIn('normal', v)
        self.assertIn('x-files', v)
        self.assertIn('uptime_in_days', v['normal'])
        self.assertIn('used_memory', v['x-files'])
        self.assertNotIn('used_memory', v['normal'])
        self.assertNotIn('uptime_in_days', v['x-files'])

    def test_config_two_with_keys(self):
        c1 = mkconf()
        c1.children.append(conf('key_task-aborts'))
        c2 = mkconf(6379, 'x-files')
        c2.children.append(conf('key_something', '_kombu.binding.celery'))
        self.r.configure_callback(c1)
        self.r.configure_callback(c2)
        self.r.read_callback()
        self.r.read_callback()
        v = self.disp_values
        self.assertIn('task-aborts', v['normal'])
        self.assertIn('something', v['x-files'])
        self.assertNotIn('something', v['normal'])
        self.assertNotIn('task-aborts', v['x-files'])


if __name__ == '__main__':
    unittest.main()

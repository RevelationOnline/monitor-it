import jsonschema
import yaml
import urllib
import requests
import jinja2

schema = {
    "type": "object",
    "properties": {
        "metrics": {"type": "object"},
        "rules": {"type": "object"},
        "notifications": {"type": "object"}
    }
}

class Metric(object):
    def __init__(self, config):
        jsonschema.validate(config, self.schema)
        self.config = self.conf(config)

    def conf(self, config):
        # process config here
        return config

    def get(self):
        raise NotImplemented

class GraphiteMetric(Metric):
    schema = {
        "type": "object",
        "properties": {
            "host": {"type": "string"},
            "query": {"type": "string"},
            "interval": {"type": "number"},

        },
        "required": ["query", "host"]
    }

    def conf(self, config):
        if config.get('interval', False) is False:
            config['interval'] = 300
        return config

    def _url(self):
        qs = urllib.parse.urlencode({
            'format': 'json',
            'target': self.config['query'],
            'from': '-%iseconds' % (self.config['interval'])
        })

        return '%s?%s' % (self.config['host'], qs)

    def get(self):
        url = self._url()
        data = requests.get(url).json()[0]['datapoints']
        # filter data
        data = [ (i, v) for (i, v) in data if i != None ]

        return data[-1][0]

class Rule(object):
    schema = {
        "type": "object",
        "properties": {
            "if": {"type": "string"},
            "message": {"type": "string"},
            "urgency": {"type": "number"}
        },
        "required": ["if"]
    }
    def __init__(self, config):
        jsonschema.validate(config, self.schema)
        self.expression = config['if'] #, '<string:' + config['if'] + '>', 'single')
        self.message_template = jinja2.Template(config.get('message', 'No message given.'))
        self.urgency = config.get('urgency', 0)


class Notification(object):
    schema = {
        "type": "object",
        "properties": {
            "notify_my_android": {
                "type": "object",
                "properties": {
                    "tokens": {
                        "type": "array",
                               "items": {
                                   'type': 'string'
                        }
                    },
                    "application": {"type": "string"},
                    "event": {"type": "string"},
                    "description": {"type": "string"},
                    "urgency": {"type": "integer"},
                    "url": {"type": "string"}
                },
                "required": ["tokens", "application", "event", "description"]
            }
        }
    }
    def __init__(self, config):
        jsonschema.validate(config, self.schema)
        self.config = self.conf(config)

    def conf(self, config):
        return config

    def run(self, messages):
        if 'notify_my_android' in self.config:
            conf = self.config['notify_my_android']
            for token in conf['tokens']:
                for urgency, message in messages:
                    if conf.get('urgency', False):
                        urgency = conf['urgency']

                    requests.post('https://www.notifymyandroid.com/publicapi/notify',data=dict(
                        apikey=token,
                        application=conf['application'],
                        event=conf['event'],
                        description=jinja2.Template(conf['description']).render(
                            messages = messages,
                        ),
                        priority=urgency,
                        url=conf.get('url', None)
                    ))



def run(config):
    metrics = {}
    rules = {}
    notifications = {}



    for name, config in data['metrics'].items():
        if config.get('graphite', False):
            metrics[name] = GraphiteMetric(config['graphite'])

    for name, config in data['rules'].items():
        rules[name] = Rule(config)

    for name, config in data['notifications'].items():
        notifications[name] = Notification(config)

    metric_values = {}
    for name, metric in metrics.items():
        metric_values[name] = metric.get()

    print(metric_values.items())

    messages = []
    for name, rule in rules.items():
        result = eval(rule.expression, dict(), metric_values.copy())

        if result:
            print(name, result, 'was true')
            params = {
                'rule_name': name
            }
            params.update(metric_values)
            messages.append((rule.urgency, rule.message_template.render(**params)))

    if len(messages) > 0:
        for name, notification in notifications.items():
            print(messages)
            notification.run(messages)


if __name__ == "__main__":
    import time
    data = yaml.load(open('config.yaml'))
    jsonschema.validate(data, schema)
    while True:
        run(data)
        time.sleep(60)


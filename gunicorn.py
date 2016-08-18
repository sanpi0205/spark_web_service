# -*- coding: utf-8 -*-
"""
Created on Tue Aug 16 14:01:13 2016

@author: zhangbo
"""

import gunicorn.app.base
from gunicorn.six import iteritems

from app import create_app
from pyspark import SparkContext, SparkConf

def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("movie_recommendation-server")
    # IMPORTANT: pass aditional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])

    return sc

class StandaloneApplication(gunicorn.app.base.BaseApplication):

    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super(StandaloneApplication, self).__init__()

    def load_config(self):
        config = dict([(key, value) for key, value in iteritems(self.options)
                       if key in self.cfg.settings and value is not None])
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application

if __name__ == "__main__":
    # Init spark context and load libraries
    sc = init_spark_context()
    #dataset_path = os.path.join('datasets', 'ml-latest')
    #dataset_path += "file://" + dataset_path
    dataset_path = "file:///root/spark-exercise/datasets/ml-latest-small"
    app = create_app(sc, dataset_path)

    # start web server
    options = {
        'bind': '%s:%s' % ('10.73.66.218', '4300'),
        'workers': 4,
    }
    StandaloneApplication(app, options).run()
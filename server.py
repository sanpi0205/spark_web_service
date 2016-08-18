# -*- coding: utf-8 -*-
"""
Created on Tue Aug 16 14:01:13 2016

@author: zhangbo
"""

import time, sys, cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf


def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("movie_recommendation-server")
    # IMPORTANT: pass aditional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])

    return sc


def run_server(app):
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)

    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')

    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 4300,
        'server.socket_host': '10.73.66.218'  #'0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()


if __name__ == "__main__":
    # Init spark context and load libraries
    sc = init_spark_context()
    #dataset_path = os.path.join('datasets', 'ml-latest')
    #dataset_path += "file://" + dataset_path
    dataset_path = "file:///root/spark-exercise/datasets/ml-latest-small"
    app = create_app(sc, dataset_path)

    # start web server
    run_server(app)
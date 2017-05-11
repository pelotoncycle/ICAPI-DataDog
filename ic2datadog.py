#!/usr/bin/env python

import json
import os
from time import sleep

from datadog import statsd
import requests
from requests.auth import HTTPBasicAuth

__author__ = 'ben.slater@instaclustr.com'

configFile = os.environ.get("INSTACLUSTER_DATADOG_CONFIG_PATH", "configuration.json")
with open(configFile) as f:
    configuration = json.load(f)


auth_details = HTTPBasicAuth(username=configuration['ic_options']['user_name'],
                             password=configuration['ic_options']['api_key'])


consecutive_fails = 0
metrics_list = configuration['metrics_list']

while True:
    for cluster_id, cluster in configuration['clusters'].iteritems():
        url = "https://api.instaclustr.com/monitoring/v1/clusters/%s?metrics=%s," % (cluster_id,
                                                                                     metrics_list)
        response = requests.get(url=url, auth=auth_details)

        if not response.ok:
            # got an error response from the Instaclustr API - raise an alert in DataDog
            # after 3 consecutive fails
            consecutive_fails += 1
            print "Error retrieving metrics from Instaclustr API: %s - %s" % (response.status_code,
                                                                              response.content)
            if consecutive_fails > 3:
                statsd.event("Instaclustr monitoring API error",
                             "Error code is: {0}".format(response.status_code))
                consecutive_fails = 0
            sleep(20)
            continue

        consecutive_fails = 0
        metrics = json.loads(response.content)
        for node in metrics:
            public_ip = node["publicIp"]
            for metric in node["payload"]:
                tags = ['cluster:%s' % cluster, 'public_ip:%s' % public_ip]
                dd_metric_name = 'instaclustr.cassandra.%s' % metric["metric"]
                if metric["metric"] == "nodeStatus":
                    # node status metric maps to a data dog service check
                    if metric["values"][0]["value"] == "WARN":
                        statsd.service_check(dd_metric_name, 1, tags=tags)  # WARN status

                    else:
                        statsd.service_check(dd_metric_name, 0, tags=tags)  # OK status

                else:
                    # all other metrics map to a data dog guage
                    statsd.gauge(dd_metric_name, metric["values"][0]["value"], tags=tags)

        sleep(20)

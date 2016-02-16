# vim: set ts=4 sw=4 tw=79 et :

import logging
import sys
import ast
import requests
import json
from influxdb import InfluxDBClusterClient
from influxdb.exceptions import InfluxDBClientError

# ###########################################################
# #### influxdb-0.9 backend  ####################################

class influxdb09(object):
    def __init__(self, cfg):
        self.log = logging.getLogger("log.backends.influxdb")
        self.log.info("InfluxDB backend initialized")
        self.scheme = "http"
        self.default_ports = {'https': 8087, 'http': 8086}
        self.timeout = 5

        if 'influxdb_use_ssl' in cfg:
            if cfg['influxdb_use_ssl']:
                self.scheme = "https"

        if 'influxdb_servers' in cfg:
            self.influxdb_servers = cfg['influxdb_servers'].split(',')
        else:
            self.influxdb_servers = ['127.0.0.1:%i' %
                                     self.default_ports[self.scheme]]

        if 'influxdb_user' in cfg:
            self.influxdb_user = cfg['influxdb_user']
        else:
            self.log.critical("Missing influxdb_user in graphios.cfg")
            sys.exit(1)

        if 'influxdb_password' in cfg:
            self.influxdb_password = cfg['influxdb_password']
        else:
            self.log.critical("Missing influxdb_password in graphios.cfg")
            sys.exit(1)

        if 'influxdb_db' in cfg:
            self.influxdb_db = cfg['influxdb_db']
        else:
            self.influxdb_db = "nagios"

        if 'influxdb_max_metrics' in cfg:
            self.influxdb_max_metrics = cfg['influxdb_max_metrics']
        else:
            self.influxdb_max_metrics = 250

        try:
            self.influxdb_max_metrics = int(self.influxdb_max_metrics)
        except ValueError:
            self.log.critical("influxdb_max_metrics needs to be a integer")
            sys.exit(1)

        if 'influxdb_extra_tags' in cfg:
            self.influxdb_extra_tags = ast.literal_eval(
                cfg['influxdb_extra_tags'])
            print self.influxdb_extra_tags
        else:
            self.influxdb_extra_tags = {}

        hosts = map(lambda x: (x.split(':')[0],x.split(':')[1]), self.influxdb_servers)
        self.client = InfluxDBClusterClient(hosts, self.influxdb_user, self.influxdb_password, timeout = self.timeout)

    def _create_database(self, name):
        """ Create database and handle possible errors """
        try:
            self.client.create_database(name)

        except InfluxDBClientError as error:
            self.log.critical("Error creating database %s: %s", name, error)
        except requests.exceptions.Timeout as error:
            self.log.critical("Timeout connecting to InfluxDB: %s", error)
            ret = 0
        except requests.exceptions.ConnectionError as error:
            self.log.critical("Error connecting to InfluxDB: %s", error)
            ret = 0
        except Exception as error:
            self.log.critical("Error creating database %s: %s", name, error)


    def send(self, metrics):
        """ Connect to influxdb and send metrics. """
        ret = 0
        perfdata = {}

        for m in metrics:
            ret += 1

            # TODO: hostcheckcommand? que pone?
            if (m.SERVICEDESC == ''):
                measurement = m.HOSTCHECKCOMMAND
                status = m.HOSTSTATE
            else:
                measurement = m.SERVICEDESC
                status = m.SERVICESTATE

            # Add project as tag
            try:
                project = m.PROJECT
            except AttributeError:
                project = "NA"

            tags = {"host": m.HOSTNAME, "project": project, "status": status}
            values = {}

            for v in m.METRICS:
                label = v['label']

                # Ensure a float gets passed
                # A measurement can not have integer and float values
                try:
                    value = float(v['value'])
                except ValueError:
                    value = 0

                if label == "time":
                    label += "_value"

                values.update({label: value})

                # Add warning and critical threshold if exists
                if v['uom']:
                    tags.update({"%s_uom" % label: v['uom']})
                if v['warn'] != '':
                    values.update({"%s_warning" % label: v['warn']})
                if v['crit'] != '':
                    values.update({"%s_critical" % label: v['crit']})
                if v['min'] != '':
                    values.update({"%s_min" % label: v['min']})
                if v['max'] != '':
                    values.update({"%s_max" % label: v['max']})

            tags.update(self.influxdb_extra_tags)

            # perfdata has each project's metrics in a different array
            perfdata.setdefault(project, []).append({
                                                     "time": int(m.TIMET),
                                                     "measurement": measurement,
                                                     "tags": tags,
                                                     "fields": values})

        for project in perfdata:
            try:
                self.client.write_points(perfdata[project], database=project,
                                         time_precision='s',
                                         batch_size = self.influxdb_max_metrics)

            except InfluxDBClientError as error:
                if "database not found" in json.loads(error.content)['error']:
                    self.log.warn("Database %s does not exist, creating...",
                            project)
                    self._create_database(project)
                else:
                    self.log.critical("Error writing points: %s", error)

            except requests.exceptions.Timeout as error:
                self.log.critical("Timeout connecting to InfluxDB: %s", error)
                ret = 0
            except requests.exceptions.ConnectionError as error:
                self.log.critical("Error connecting to InfluxDB: %s", error)
                ret = 0
            except Exception as error:
                self.log.critical("Error writing points to InfluxDB: %s", error)
                # If send fails, set sended metrics as 0.
                # Will print a error message: "...insufficent metrics sent from..."
                ret = 0

        return ret


# ###########################################################
# #### stdout backend  #######################################

class stdout(object):
    def __init__(self, cfg):
        self.log = logging.getLogger("log.backends.stdout")
        self.log.info("STDOUT Backend Initialized")

    def send(self, metrics):
        ret = 0
        for metric in metrics:
            ret += 1
            for m in metric.METRICS:
                print("%s:%s" % ('LABEL', m['label']))
                print("%s:%s" % ('VALUE ', m['value']))
                print("%s:%s" % ('UOM ', m['uom']))
            print("%s:%s" % ('DATATYPE ', metric.DATATYPE))
            print("%s:%s" % ('TIMET ', metric.TIMET))
            print("%s:%s" % ('HOSTNAME ', metric.HOSTNAME))
            print("%s:%s" % ('SERVICEDESC ', metric.SERVICEDESC))
            print("%s:%s" % ('PERFDATA ', metric.PERFDATA))
            print("%s:%s" % ('SERVICECHECKCOMMAND',
                             metric.SERVICECHECKCOMMAND))
            print("%s:%s" % ('HOSTCHECKCOMMAND ', metric.HOSTCHECKCOMMAND))
            print("%s:%s" % ('HOSTSTATE ', metric.HOSTSTATE))
            print("%s:%s" % ('HOSTSTATETYPE ', metric.HOSTSTATETYPE))
            print("%s:%s" % ('SERVICESTATE ', metric.SERVICESTATE))
            print("%s:%s" % ('SERVICESTATETYPE ', metric.SERVICESTATETYPE))
            print("%s:%s" % ('METRICBASEPATH ', metric.METRICBASEPATH))
            print("%s:%s" % ('GRAPHITEPREFIX ', metric.GRAPHITEPREFIX))
            print("%s:%s" % ('GRAPHITEPOSTFIX ', metric.GRAPHITEPOSTFIX))
            print("-------")

        return ret


# ###########################################################
# #### start here  #######################################

if __name__ == "__main__":
    print("I'm just a lowly module. Try calling graphios.py instead")
    sys.exit(42)

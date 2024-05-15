#!/usr/bin/env python3

from influxdb import InfluxDBClient
from datetime import datetime, timedelta
from os import path
import sys
import os
import serial
import time
import yaml
import logging
import modbus_tk
import modbus_tk.defines as cst
from modbus_tk import modbus_rtu
from modbus_tk import modbus_tcp
import struct

PORT = 'COM3'
#PORT = '/dev/ttyUSB0'
#PORT = '/dev/serial/by-id/usb-1a86_USB2.0-Serial-if00-port0'

# Change working dir to the same dir as this script
os.chdir(sys.path[0])

class DataCollector:
    def __init__(self, influx_yaml, meter_yaml):
        self.influx_yaml = influx_yaml
        self.influx_map = None
        self.influx_map_last_change = -1
        self.influx_inteval_save = dict()
        log.info('InfluxDB:')
        for influx_config in sorted(self.get_influxdb(), key=lambda x:sorted(x.keys())):
            log.info('\t {} <--> {} , Interval: {}'.format(influx_config['host'], influx_config['name'], influx_config['interval']))
        self.meter_yaml = meter_yaml
        self.max_iterations = None  # run indefinitely by default
        self.meter_map = None
        self.meter_map_last_change = -1
        log.info('Meters:')
        for meter in sorted(self.get_meters(), key=lambda x:sorted(x.keys())):
            log.info('\t {} <--> {}'.format( meter['id'], meter['name']))

    def get_meters(self):
        assert path.exists(self.meter_yaml), 'Meter map not found: %s' % self.meter_yaml
        if path.getmtime(self.meter_yaml) != self.meter_map_last_change:
            try:
                log.info('Reloading meter map as file changed')
                new_map = yaml.load(open(self.meter_yaml), Loader=yaml.FullLoader)
                self.meter_map = new_map['meters']
                self.meter_map_last_change = path.getmtime(self.meter_yaml)
            except Exception as e:
                log.warning('Failed to re-load meter map, going on with the old one.')
                log.warning(e)
        return self.meter_map

    def get_influxdb(self):
        assert path.exists(self.influx_yaml), 'InfluxDB map not found: %s' % self.influx_yaml
        if path.getmtime(self.influx_yaml) != self.influx_map_last_change:
            try:
                log.info('Reloading influxDB map as file changed')
                new_map = yaml.load(open(self.influx_yaml), Loader=yaml.FullLoader)
                self.influx_map = new_map['influxdb']
                self.influx_map_last_change = path.getmtime(self.influx_yaml)
                list = 0
                for influx_config in sorted(self.get_influxdb(), key=lambda x:sorted(x.keys())):
                    list = list + 1
                    self.influx_inteval_save[list] = influx_config['interval']
            except Exception as e:
                log.warning('Failed to re-load influxDB map, going on with the old one.')
                log.warning(e)
        return self.influx_map

    def collect_and_store(self):
        meters = self.get_meters()
        influxdb = self.get_influxdb()
        t_utc = datetime.utcnow()
        t_str = t_utc.isoformat() + 'Z'

        datas = dict()
        meter_id_name = dict() # mapping id to name
        meter_slave_id = dict()
        list = 0 # mapping list to id

        for meter in meters:
            list = list + 1
            meter_id_name[list] = meter['name']
            meter_slave_id[list] = meter['id']

            try:
                if meter['conexion'] == 'R':
                    masterRTU = modbus_rtu.RtuMaster(
                        serial.Serial(port=PORT, baudrate=meter['baudrate'], bytesize=meter['bytesize'], parity=meter['parity'], stopbits=meter['stopbits'], xonxoff=0)
                    )

                    masterRTU.set_timeout(meter['timeout'])
                    masterRTU.set_verbose(True)

                    log.debug('Reading meter %s.' % (meter['name']))
                    start_time = time.time()
                    parameters = yaml.load(open(meter['type']), Loader=yaml.FullLoader)
                    datas[list] = dict()

                    for parameter in parameters:
                        # If random readout errors occour, e.g. CRC check fail, test to uncomment the following row
                        time.sleep(0.2) # Sleep for 150 ms between each parameter read to avoid errors
                        retries = 3
                        while retries > 0:
                            try:
                                retries -= 1
                                if meter['function'] == 3:
                                    if parameters[parameter][2] == 1:
                                        resultado = masterRTU.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>f')
                                    elif parameters[parameter][2] == 2:
                                        resultado = masterRTU.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>l')
                                    elif parameters[parameter][2] == 3:
                                        resultado = masterRTU.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1])
                                    elif parameters[parameter][2] == 4:
                                        resultadoTemp = masterRTU.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1])
                                        resultado = [0,0]
                                        resultado[0] = (resultadoTemp[1]<<16)|resultadoTemp[0]
                                    elif parameters[parameter][2] == 5:
                                        resultado = masterRTU.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>I')
                                    elif parameters[parameter][2] == 6:
                                        resultado = masterRTU.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>L')
                                    elif parameters[parameter][2] == 7:
                                        resultadoTemp = masterRTU.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1])
                                        resultado = [0,0]
                                        resultado[0] = struct.unpack('f', struct.pack('I', (resultadoTemp[1]<<16)|resultadoTemp[0]))[0]
                                elif meter['function'] == 4:
                                    if parameters[parameter][2] == 1:
                                        resultado = masterRTU.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>f')
                                    elif parameters[parameter][2] == 2:
                                        resultado = masterRTU.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>l')
                                    elif parameters[parameter][2] == 3:
                                        resultado = masterRTU.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1])
                                    elif parameters[parameter][2] == 4:
                                        resultadoTemp = masterRTU.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1])
                                        resultado = [0,0]
                                        resultado[0] = (resultadoTemp[1]<<16)|resultadoTemp[0]
                                    elif parameters[parameter][2] == 5:
                                        resultado = masterRTU.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>I')
                                    elif parameters[parameter][2] == 6:
                                        resultado = masterRTU.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>L')
                                    elif parameters[parameter][2] == 7:
                                        resultadoTemp = masterRTU.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1])
                                        resultado = [0,0]
                                        resultado[0] = struct.unpack('f', struct.pack('I', (resultadoTemp[1]<<16)|resultadoTemp[0]))[0]
                                datas[list][parameter] = resultado[0]
                                retries = 0
                                pass
                            except ValueError as ve:
                                log.warning('Value Error while reading register {} from meter {}. Retries left {}.'
                                       .format(parameters[parameter], meter['id'], retries))
                                log.error(ve)
#                                if retries == 0:
#                                    raise RuntimeError
                            except TypeError as te:
                                log.warning('Type Error while reading register {} from meter {}. Retries left {}.'
                                       .format(parameters[parameter], meter['id'], retries))
                                log.error(te)
#                                if retries == 0:
#                                    raise RuntimeError
                            except IOError as ie:
                                log.warning('IO Error while reading register {} from meter {}. Retries left {}.'
                                       .format(parameters[parameter], meter['id'], retries))
                                log.error(ie)
#                                if retries == 0:
#                                    raise RuntimeError
                            except:
                                log.error("Unexpected error:", sys.exc_info()[0])
#                                raise

                    datas[list]['ReadTime'] =  time.time() - start_time
                    del masterRTU;
                elif meter['conexion'] == 'T':
                    masterTCP = modbus_tcp.TcpMaster(host=meter['direction'],port=meter['port'])

                    masterTCP.set_timeout(meter['timeout'])

                    log.debug('Reading meter %s.' % (meter['name']))
                    start_time = time.time()
                    parameters = yaml.load(open(meter['type']), Loader=yaml.FullLoader)
                    datas[list] = dict()

                    for parameter in parameters:
                        # If random readout errors occour, e.g. CRC check fail, test to uncomment the following row
                        time.sleep(0.2) # Sleep for 150 ms between each parameter read to avoid errors
                        retries = 3
                        while retries > 0:
                            try:
                                retries -= 1
                                if meter['function'] == 3:
                                    if parameters[parameter][2] == 1:
                                        resultado = masterTCP.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>f')
                                    elif parameters[parameter][2] == 2:
                                        resultado = masterTCP.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>l')
                                    elif parameters[parameter][2] == 3:
                                        resultado = masterTCP.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1])
                                    elif parameters[parameter][2] == 4:
                                        resultadoTemp = masterTCP.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1])
                                        resultado = [0,0]
                                        resultado[0] = (resultadoTemp[1]<<16)|resultadoTemp[0]
                                    elif parameters[parameter][2] == 5:
                                        resultado = masterTCP.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>I')
                                    elif parameters[parameter][2] == 6:
                                        resultado = masterTCP.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>L')
                                    elif parameters[parameter][2] == 7:
                                        resultadoTemp = masterRTU.execute(meter['id'], cst.READ_HOLDING_REGISTERS, parameters[parameter][0], parameters[parameter][1])
                                        resultado = [0,0]
                                        resultado[0] = struct.unpack('f', struct.pack('I', (resultadoTemp[1]<<16)|resultadoTemp[0]))[0]
                                elif meter['function'] == 4:
                                    if parameters[parameter][2] == 1:
                                        resultado = masterTCP.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>f')
                                    elif parameters[parameter][2] == 2:
                                        resultado = masterTCP.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>l')
                                    elif parameters[parameter][2] == 3:
                                        resultado = masterTCP.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1])
                                    elif parameters[parameter][2] == 4:
                                        resultadoTemp = masterTCP.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1])
                                        resultado = [0,0]
                                        resultado[0] = (resultadoTemp[1]<<16)|resultadoTemp[0]
                                    elif parameters[parameter][2] == 5:
                                        resultado = masterTCP.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>I')
                                    elif parameters[parameter][2] == 6:
                                        resultado = masterTCP.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1], data_format='>L')
                                    elif parameters[parameter][2] == 7:
                                        resultadoTemp = masterRTU.execute(meter['id'], cst.READ_INPUT_REGISTERS, parameters[parameter][0], parameters[parameter][1])
                                        resultado = [0,0]
                                        resultado[0] = struct.unpack('f', struct.pack('I', (resultadoTemp[1]<<16)|resultadoTemp[0]))[0]
                                datas[list][parameter] = resultado[0]
                                retries = 0
                                pass
                            except ValueError as ve:
                                log.warning('Value Error while reading register {} from meter {}. Retries left {}.'
                                       .format(parameters[parameter], meter['id'], retries))
                                log.error(ve)
#                                if retries == 0:
#                                    raise RuntimeError
                            except TypeError as te:
                                log.warning('Type Error while reading register {} from meter {}. Retries left {}.'
                                       .format(parameters[parameter], meter['id'], retries))
                                log.error(te)
#                                if retries == 0:
#                                    raise RuntimeError
                            except IOError as ie:
                                log.warning('IO Error while reading register {} from meter {}. Retries left {}.'
                                       .format(parameters[parameter], meter['id'], retries))
                                log.error(ie)
#                                if retries == 0:
#                                    raise RuntimeError
                            except:
                                log.error("Unexpected error:", sys.exc_info()[0])
#                                raise

                    datas[list]['ReadTime'] =  time.time() - start_time
                    del masterTCP;
            except modbus_tk.modbus.ModbusError as exc:
                log.error("%s- Code=%d", exc, exc.get_exception_code())


        json_body = [
            {
                'measurement': meter_id_name[meter_id],
                'tags': {
                    'id': meter_slave_id[meter_id],
                    'meter': meter_id_name[meter_id],
                },
                'time': t_str,
                'fields': datas[meter_id]
            }
            for meter_id in datas
        ]
        
        if len(json_body) > 0:

#            log.debug(json_body)

            list = 0

            for influx_config in influxdb:
                list = list + 1
                if self.influx_inteval_save[list] > 0:
                    if self.influx_inteval_save[list] <= 1:
                        self.influx_inteval_save[list] = influx_config['interval']

                        DBclient = InfluxDBClient(influx_config['host'],
                                                influx_config['port'],
                                                influx_config['user'],
                                                influx_config['password'],
                                                influx_config['dbname'])
                        try:
                            DBclient.write_points(json_body)
                            log.info(t_str + ' Data written for %d meters in {}.' .format(influx_config['name']) % len(json_body) )
                        except Exception as e:
                            log.error('Data not written! in {}' .format(influx_config['name']))
                            log.error(e)
                            raise
                    else:
                        self.influx_inteval_save[list] = self.influx_inteval_save[list] - 1
        else:
            log.warning(t_str, 'No data sent.')


def repeat(interval_sec, max_iter, func, *args, **kwargs):
    from itertools import count
    import time
    starttime = time.time()
    for i in count():
        if interval_sec > 0:
            time.sleep(interval_sec - ((time.time() - starttime) % interval_sec))
        if i % 1000 == 0:
            log.info('Collected %d readouts' % i)
        try:
            func(*args, **kwargs)
        except Exception as ex:
            log.error(ex)
        if max_iter and i >= max_iter:
            return


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', default=60,
                        help='Meter readout interval (seconds), default 60')
    parser.add_argument('--meters', default='meters.yml',
                        help='YAML file containing Meter ID, name, type etc. Default "meters.yml"')
    parser.add_argument('--influxdb', default='influx_config.yml',
                        help='YAML file containing Influx Host, port, user etc. Default "influx_config.yml"')
    parser.add_argument('--log', default='CRITICAL',
                        help='Log levels, DEBUG, INFO, WARNING, ERROR or CRITICAL')
    parser.add_argument('--logfile', default='',
                        help='Specify log file, if not specified the log is streamed to console')
    args = parser.parse_args()
    interval = int(args.interval)
    loglevel = args.log.upper()
    logfile = args.logfile

    # Setup logging
    log = logging.getLogger('energy-logger')
    log.setLevel(getattr(logging, loglevel))

    if logfile:
        loghandle = logging.FileHandler(logfile, 'w')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        loghandle.setFormatter(formatter)
    else:
        loghandle = logging.StreamHandler()

    log.addHandler(loghandle)

    log.info('Started app')

    collector = DataCollector(influx_yaml=args.influxdb,
                              meter_yaml=args.meters)

    repeat(interval,
           max_iter=collector.max_iterations,
           func=lambda: collector.collect_and_store())

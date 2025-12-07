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
from modbus_tk import modbus_rtu, modbus_tcp
import struct

PORT = 'COM3'          # ← change to your real port if needed
# PORT = '/dev/ttyUSB0'

os.chdir(sys.path[0])

log = logging.getLogger('energy-logger')

class DataCollector:
    def __init__(self, influx_yaml, meter_yaml):
        self.influx_yaml = influx_yaml
        self.influx_map = None
        self.influx_map_last_change = -1
        self.influx_interval_save = {}
        self.meter_yaml = meter_yaml
        self.meter_map = None
        self.meter_map_last_change = -1
            
        log.info('InfluxDB configurations:')
        for cfg in self.get_influxdb():
            log.info(f"  • {cfg['name']} → {cfg['host']} (every {cfg['interval']} cycles)")

        log.info('Meters:')
        for m in self.get_meters():
            log.info(f"  • ID {m['id']:>3} → {m['name']} ({'TCP' if m.get('conexion')=='T' else 'RTU'})")

    def get_meters(self):
        if not path.exists(self.meter_yaml):
            log.error(f'Meter file not found: {self.meter_yaml}')
            sys.exit(1)
        if path.getmtime(self.meter_yaml) != self.meter_map_last_change:
            with open(self.meter_yaml) as f:
                self.meter_map = yaml.load(f, Loader=yaml.FullLoader)['meters']
                self.meter_map_last_change = path.getmtime(self.meter_yaml)
                log.info('Meter map reloaded')
        return self.meter_map

    def get_influxdb(self):
        if not path.exists(self.influx_yaml):
            log.error(f'Influx config not found: {self.influx_yaml}')
            sys.exit(1)
        if path.getmtime(self.influx_yaml) != self.influx_map_last_change:
            with open(self.influx_yaml) as f:
                self.influx_map = yaml.load(f, Loader=yaml.FullLoader)['influxdb']
                self.influx_map_last_change = path.getmtime(self.influx_yaml)
                self.influx_interval_save = {i+1: cfg['interval'] for i, cfg in enumerate(self.influx_map)}
                log.info('InfluxDB config reloaded')
        return self.influx_map
        
    def safe_read_registers(self, master, slave_id, func_code, start_addr, count, dtype):
        """Read registers safely – returns value or None if meter does not answer."""
        for attempt in range(3):
            try:
                raw = master.execute(slave_id, func_code, start_addr, count)

                if dtype == 1:      # float big-endian
                    return struct.unpack('>f', struct.pack('>HH', raw[0], raw[1]))[0]
                if dtype == 2:      # signed 32-bit
                    return struct.unpack('>l', struct.pack('>HH', raw[0], raw[1]))[0]
                if dtype == 3:      # raw registers
                    return raw[0] if len(raw) == 1 else list(raw)
                if dtype == 4:      # swapped word order 32-bit
                    return (raw[1] << 16) | raw[0]
                if dtype == 5:      # unsigned 32-bit
                    return struct.unpack('>I', struct.pack('>HH', raw[0], raw[1]))[0]
                if dtype == 6:      # unsigned 64-bit (4 registers)
                    return struct.unpack('>Q', struct.pack('>HHHH', *raw))[0]   # ← fixed line
                return raw[0]   # fallback
            except Exception as e:
                if attempt == 2:
                    log.debug(f"Meter ID {slave_id} addr {start_addr}: {e}")
                time.sleep(0.08)
        return None


    def collect_and_store(self):
        meters = self.get_meters()
        influx_cfgs = self.get_influxdb()
        t_utc = datetime.utcnow().isoformat() + 'Z'

        datas = {}
        meter_id_name = {}
        meter_slave_id = {}
        idx = 0

        for meter in meters:
            idx += 1
            meter_id_name[idx] = meter['name']
            meter_slave_id[idx] = meter['id']

            try:
                # Create master (RTU or TCP)
                if meter.get('conexion') == 'R':
                    ser = serial.Serial(
                        port=PORT,
                        baudrate=meter['baudrate'],
                        bytesize=meter['bytesize'],
                        parity=meter['parity'],
                        stopbits=meter['stopbits'],
                        timeout=1
                    )
                    master = modbus_rtu.RtuMaster(ser)
                elif meter.get('conexion') == 'T':
                    master = modbus_tcp.TcpMaster(host=meter['direction'], port=meter.get('port', 502))
                else:
                    log.warning(f"Unknown conexion {meter.get('conexion')} for {meter['name']}")
                    continue

                master.set_timeout(meter.get('timeout', 2.0))

                log.debug(f"Reading {meter['name']} (ID {meter['id']})")
                start_time = time.time()

                with open(meter['type']) as f:
                    parameters = yaml.load(f, Loader=yaml.FullLoader)

                datas[idx] = {'ReadTime': 0.0}
                func_code = cst.READ_HOLDING_REGISTERS if meter['function'] == 3 else cst.READ_INPUT_REGISTERS

                for param_name, param_def in parameters.items():
                    time.sleep(0.15)
                    value = self.safe_read_registers(
                        master, meter['id'], func_code,
                        param_def[0], param_def[1], param_def[2]
                    )
                    datas[idx][param_name] = value

                datas[idx]['ReadTime'] = time.time() - start_time
                master._do_close()

            except Exception as e:
                log.error(f"Failed meter {meter.get('name','?')} (ID {meter['id']}): {e}")
                
                
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
            
            
        # Write to InfluxDB
        if datas:
            json_body = [
                {
                    "measurement": meter_id_name[i],
                    "tags": {"id": str(meter_slave_id[i])},
                    "time": t_utc,
                    "fields": {k: float(v) if isinstance(v, (int,float)) and v is not None else 0.0
                               for k, v in datas[i].items() if k != 'ReadTime'}
                }
                for i in datas
            ]

            if json_body:
                for n, cfg in enumerate(influx_cfgs, 1):
                    if self.influx_interval_save.get(n, 0) <= 1:
                        self.influx_interval_save[n] = cfg['interval']
                        
                        try:  
                            DBclient = InfluxDBClient(cfg['host'],
                                                    cfg['port'],
                                                    cfg['user'],
                                                    cfg['password'],
                                                    cfg['dbname'])
                            DBclient.write_points(json_body)
                           
                        except Exception as e:
                            log.error(f"Influx write error ({cfg['name']}): {e}")
                    else:
                        self.influx_interval_save[n] -= 1


def repeat(interval_sec, func):
    import time
    next_time = time.time()
    counter = 0
    while True:
        try:
            func()
        except Exception as e:
            log.exception(f"Unhandled exception: {e}")

        counter += 1
        if counter % 100 == 0:
            log.info(f"Ran {counter} cycles")

        next_time += interval_sec
        sleep_time = next_time - time.time()
        if sleep_time > 0:
            time.sleep(sleep_time)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', type=int, default=60)
    parser.add_argument('--meters', default='meters.yml')
    parser.add_argument('--influxdb', default='influx_config.yml')
    parser.add_argument('--log', default='INFO',
                        choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'])
    parser.add_argument('--logfile', default='')
    args = parser.parse_args()

    log.setLevel(getattr(logging, args.log.upper()))
    handler = logging.FileHandler(args.logfile) if args.logfile else logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    log.addHandler(handler)

    log.info('Energy meter logger started')
    collector = DataCollector(influx_yaml=args.influxdb, meter_yaml=args.meters)
    repeat(interval_sec=args.interval, func=collector.collect_and_store)

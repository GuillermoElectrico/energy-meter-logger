#!/bin/bash 

echo "Pre Installation easy script (Step-by-step instructions) to debian x86_64/ARMv7"
echo "Install InfluxDB"
echo "Add the InfluxData repository"
curl -sL https://repos.influxdata.com/influxdb.key | sudo apt-key add -
source /etc/os-release
test $VERSION_ID = "9" && echo "deb https://repos.influxdata.com/debian stretch stable" | sudo tee /etc/apt/sources.list.d/influxdb.list
echo "Download and install"
sudo apt-get update && sudo apt-get install influxdb
echo "Start the influxdb service"
sudo service influxdb start
echo "Install Grafana"
echo "Add APT Repository"
echo "deb https://packages.grafana.com/oss/deb stable main" | sudo tee -a /etc/apt/sources.list.d/grafana.list
echo "Add Bintray key"
curl https://packages.grafana.com/gpg.key | sudo apt-key add -
echo "Now install"
sudo apt-get update && sudo apt-get install grafana
echo "Start the service using systemd"
sudo systemctl daemon-reload
sudo systemctl start grafana-server
systemctl status grafana-server
echo "Enable the systemd service so that Grafana starts at boot"
sudo systemctl enable grafana-server.service
echo "Go to http://localhost:3000 and login using admin / admin (remember to change password)"
echo "Install Energy Meter Logger"
echo "Download and install from Github and install pip3"
sudo apt-get install git
git clone https://github.com/GuillermoElectrico/energy-meter-logger
sudo apt-get install python3-pip
echo "Run setup script"
cd energy-meter-logger
sudo pip3 install setuptools
sudo python3 setup.py install
echo "Make script file executable"
chmod 777 read_energy_meter.py
echo "END Pre-Instalacion"
echo "Edit meters.yml and influx_config.yml to match your configuration and create database in Influx"
echo "Continue step-by-step since [Test the configuration by running:] in https://github.com/GuillermoElectrico/energy-meter-logger#install-energy-meter-logger"
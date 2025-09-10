#!/bin/bash

cd $(dirname $0)
source bin/activate
#echo `date +\%F-\%R` `python /root/latency-monitoring/check_products.py` >> /var/dhus/latency-monitoring.log
python /root/latency-monitoring/check_products.py >> /dev/null

#!/bin/bash
# Setting container timezone to Brazil
echo Brazil/East >/etc/timezone && ln -sf /usr/share/zoneinfo/Brazil/East /etc/localtime && dpkg-reconfigure -f noninteractive tzdata

# Initializing worker
python move2cloud.py --migration_id $id --database_password $senha --database_port $port --database_ip $ip




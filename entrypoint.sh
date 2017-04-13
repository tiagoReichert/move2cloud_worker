#!/bin/bash

echo Brazil/East >/etc/timezone && ln -sf /usr/share/zoneinfo/Brazil/East /etc/localtime && dpkg-reconfigure -f noninteractive tzdata
python move2cloud.py --migration_id $id --database_pasword $senha --database_port $port --database_ip $ip




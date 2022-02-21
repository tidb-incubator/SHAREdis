#!/bin/bash

#额外执行命令
eval ${Ext_CMD}

# 以对应用启动时，1）部分文件没有权限；2）/data/logs 挂载到容器后普通用户无写权限
sudo chown -R ${AppUser}:${AppGroup} /data
ln -s /data/logs log

pd_addr="\"10.20.14.42:2379,10.20.14.243:2379,10.20.0.176:2379\""
if [ "${Service_name}" == "hermes-ro-sg1" ]
then
    pd_addr="\"172.26.49.78:2379,172.26.51.158:2379,172.26.76.104:2379\""
elif [ "${Service_name}" == "hermes-ro-sg2" ]
then
    pd_addr="\"10.21.22.247:2379,10.21.47.161:2379,10.21.6.153:2379\""
fi
sed -i "s/PD_ADDR/${pd_addr}/g" /data/sharedis/config/config-${Env}.toml

cp /data/sharedis/config/config-${Env}.toml /data/sharedis/config/config.toml
exec /data/sharedis/bin/sharedis --conf /data/sharedis/config/config.toml

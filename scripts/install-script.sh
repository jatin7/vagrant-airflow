#!/usr/bin/env bash
# run as root
echo "====== START ======"
sudo su

apt-get update

apt -y install postgresql postgresql-contrib

sudo systemctl start postgresql.service

# installing PostgreSQL and preparing the database / VERSION 9.5 (or higher)

echo "CREATE USER airflow PASSWORD 'airflow'; CREATE DATABASE airflow; GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;" | sudo -u postgres psql
sudo -u postgres sed -i "s|#listen_addresses = 'localhost'|listen_addresses = '*'|" /etc/postgresql/*/main/postgresql.conf
sudo -u postgres sed -i "s|127.0.0.1/32|0.0.0.0/0|" /etc/postgresql/*/main/pg_hba.conf
sudo -u postgres sed -i "s|::1/128|::/0|" /etc/postgresql/*/main/pg_hba.conf
#service postgresql restart


# installing python 3.x and dependencies
apt-get update
apt-get -y install python3 python3-dev python3-pip python3-wheel
pip install --upgrade pip
#pip install -r ../requirements.txt
pip install apache-airflow==2.8.4
pip install apache-airflow-providers-apache-hive==7.0.1
pip install apache-airflow-providers-apache-livy==3.7.3
pip install psycopg2-binary==2.9.9
pip install click==8.1.7
pip install black==24.3.0


# create airflow user with sudo capability
adduser airflow --gecos "airflow,,," --disabled-password
echo "airflow:airflow" | chpasswd
usermod -aG sudo airflow
echo "airflow ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

AIRFLOW_HOME=/opt/airflow

sudo cp ../../vagrant/airflow /opt/ -r

# airflow local
mkdir -p $AIRFLOW_HOME
mkdir -p /var/log/airflow $AIRFLOW_HOME/dags $AIRFLOW_HOME/plugins $AIRFLOW_HOME/logs
sudo chmod -R 777 $AIRFLOW_HOME
sudo chown -R airflow:airflow $AIRFLOW_HOME
sudo chown airflow /var/log/airflow



declare -a arr=(
"AIRFLOW_HOME=/opt/airflow" 
"AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION='true'" 
"AIRFLOW__CORE__LOAD_EXAMPLES='false'" 
"AIRFLOW__CORE__DEFAULT_UI_TIMEZONE='America/Sao_Paulo'" 
"AIRFLOW__CORE__DEFAULT_TIMEZONE='America/Sao_Paulo'" 
"AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE='America/Sao_Paulo'" 
"AIRFLOW__WEBSERVER__INSTANCE_NAME='SHELL_AIRFLOW'" 
"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost:5432/airflow" 
"AIRFLOW__CORE__EXECUTOR=LocalExecutor" 
)
## clean file
truncate -s 0 /etc/profile.d/airflow.sh
## now loop through the above array
for i in "${arr[@]}"
do
   echo export "$i" >> /etc/profile.d/airflow.sh
done

truncate -s 0 /tmp/airflow_environment
for i in "${arr[@]}"
do
   echo "$i" >> /tmp/airflow_environment
done


cat /tmp/airflow_environment | sudo tee -a /etc/default/airflow

# following commands should be run under airflow user
echo "===== install airflow ======"
su - airflow 
source /etc/profile.d/airflow.sh
echo $AIRFLOW_HOME
airflow db migrate

airflow users create \
--username admin \
--firstname Jacob \
--password admin \
--lastname Huang \
--role Admin \
--email objecthuang@hotmail.com

echo "====== airflow service ====="
# # setting up Airflow 
sudo su
sudo tee -a /usr/bin/airflow-webserver <<EOL
#!/usr/bin/env bash
airflow webserver
EOL

sudo tee -a /usr/bin/airflow-scheduler <<EOL
#!/usr/bin/env bash
airflow scheduler
EOL

#sudo tee -a /usr/bin/airflow-worker <<EOL
#!/usr/bin/env bash
#airflow celery worker
#EOL

chmod 755 /usr/bin/airflow-webserver
chmod 755 /usr/bin/airflow-scheduler
#chmod 755 /usr/bin/airflow-worker

echo "
[Unit]
Description=Airflow daemon
After=network.target

[Service]
EnvironmentFile=/etc/default/airflow
User=airflow
Group=airflow
Type=simple
Restart=always
ExecStart=/usr/bin/airflow-webserver
RestartSec=5s
PrivateTmp=true

[Install]
WantedBy=multi-user.target
    " >> /tmp/webserver.service

echo "
[Unit]
Description=Airflow daemon
After=network.target

[Service]
EnvironmentFile=/etc/default/airflow
User=airflow
Group=airflow
Type=simple
Restart=always
ExecStart=/usr/bin/airflow-scheduler
RestartSec=5s
PrivateTmp=true

[Install]
WantedBy=multi-user.target
    " >> /tmp/scheduler.service

# echo "
# [Unit]
# Description=Airflow daemon
# After=network.target

# [Service]
# EnvironmentFile=/etc/default/airflow
# User=airflow
# Group=airflow
# Type=simple
# Restart=always
# ExecStart=/usr/bin/airflow-worker
# RestartSec=5s
# PrivateTmp=true

# [Install]
# WantedBy=multi-user.target
#     " >> /tmp/worker.service

sudo cat /tmp/webserver.service >> /etc/systemd/system/webserver.service
sudo cat /tmp/scheduler.service >> /etc/systemd/system/scheduler.service
#sudo cat /tmp/worker.service >> /etc/systemd/system/worker.service

sudo systemctl enable webserver.service
sudo systemctl start webserver.service
#sudo systemctl status webserver.service
#journalctl -u webserver.service -b
sudo systemctl enable scheduler.service
sudo systemctl start scheduler.service
#sudo systemctl restart scheduler.service
#sudo systemctl status scheduler.service
#journalctl -u scheduler.service -b
#sudo systemctl enable worker.service
#sudo systemctl start worker.service
#sudo systemctl status worker.service
#journalctl -u worker.service -b
apt-get purge --auto-remove -yqqq
apt-get autoremove -yqq --purge
apt-get clean

chmod -R 777 $AIRFLOW_HOME
chown -R airflow:airflow $AIRFLOW_HOME
echo "====== END ======="
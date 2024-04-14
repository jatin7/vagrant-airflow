# Vagrant + Airflow

Vagrant to start a airflow 2.8.4 virtual machine

# Vagrant + Airflow

main script to install airflow: [install-script.sh](./scripts/install-script.sh)

```
# start the virtual macine
vagrant up

# acessar a vm
vagrant ssh

# remove vm
vagrant destroy -f

# re-run the install-script.sh, after you created virtual machine
vagrant provision
```

## Dags
[dags](./dags/) same sample dag to verify airflow is working as expected.

To visit the airflow web UI:
http://192.168.2.21:8080
username/password admin/admin
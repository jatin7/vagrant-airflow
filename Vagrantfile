# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
 
  config.vm.box = "bento/ubuntu-22.04" 
  config.vm.network:private_network, ip: "192.168.2.21"
  config.vm.network:public_network
  config.vm.hostname = "airflow"

#  config.vm.synced_folder "./dags", "/opt/airflow/dags"

  config.vm.provider "virtualbox" do |vb|
     vb.memory = "4096"
  end
  config.vm.provision "shell", path: "./scripts/install-script.sh"
end

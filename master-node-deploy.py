from fabric.api import *
from fabric.context_managers import cd
from fabric.contrib.files import exists

env.hosts = ["192.168.85.137"]
env.slaves = ["192.168.85.136"]
env.user = "ubuntu"
env.password = "Oogae3th"
env.port = 22

local_path = "templete/master/"
remote_path = "/usr/local/hadoop/"

def addSlaveHostNames():
    sudo("echo " + str(env.hosts[0]) + "\thadoopmaster >> /etc/hosts");
    count = 0;
    for slave in env.slaves:
        count = count + 1;
        sudo("echo " + str(slave) + "\thadoopslave" + str(count) + " >> /etc/hosts");

def changeHostName():
    sudo("echo hadoopmaster > /etc/hostname");

def copyConfigFiles():
    put(local_path + "core-site.xml", remote_path + "etc/hadoop/", use_sudo=True);
    put(local_path + "hdfs-site.xml", remote_path + "etc/hadoop/", use_sudo=True);
    put(local_path + "mapred-site.xml", remote_path + "etc/hadoop/", use_sudo=True);
    put(local_path + "yarn-site.xml", remote_path + "etc/hadoop/", use_sudo=True);

def addMasterSlaveNodeName():
    sudo("echo hadoopmaster > " + remote_path + "etc/hadoop/masters");
    count = 0;
    sudo("rm " + remote_path + "etc/hadoop/slaves");
    for slave in env.slaves:
        count = count + 1;
        sudo("echo hadoopslave" + str(count) + " >> " + remote_path + "etc/hadoop/slaves");
        
def cleanNameNodeDir():
    if exists(remote_path + "hadoop-data/hdfs/namenode", use_sudo=True):
        sudo("rm -r " + remote_path + "hadoop-data/hdfs/namenode");
        sudo("mkdir -p " + remote_path + "hadoop-data/hdfs/namenode");
    else:
        sudo("mkdir -p " + remote_path + "hadoop-data/hdfs/namenode");

def permissionToUser():
    sudo("chown -R hadoop:hadoop " + remote_path);

def sshConnectionConfiguration():
    execute(su, env.password, 'hadoop', 'ssh-keygen -t rsa')
    
    execute(su, env.password, 'hadoop', 'ssh-copy-id -i ~/.ssh/id_rsa.pub hadoopmaster')
#     execute(su, env.password, 'hadoop', "cat ~/.ssh/id_rsa.pub | ssh ubuntu@hadoopmaster 'cat >> ~/.ssh/authorized_keys'")
    
#     count = 0;
#     for slave in env.slaves:
#         count = count + 1
#         execute(su, env.password, 'hadoop', "ssh ubuntu@hadoopslave" + str(count) + " mkdir -p ~/.ssh")
#         execute(su, env.password, 'hadoop', "cat ~/.ssh/id_rsa.pub | ssh ubuntu@hadoopslave" + str(count) + " 'cat >> ~/.ssh/authorized_keys'")

def cleanNamenode():
    execute(su, env.password, 'hadoop', 'hadoop namenode -format') 

def startAllService():
    execute(su, env.password, 'hadoop', 'start-all.sh')
    
def su(pwd, user, command):
    with settings(
        password="%s" % pwd,
        sudo_prefix="su %s -c " % user,
        sudo_prompt="Password:"
        ):
        sudo(command)
         
def main():
#     addSlaveHostNames();
#     changeHostName();
#     copyConfigFiles();
#     addMasterSlaveNodeName();
#     cleanNameNodeDir();
#     permissionToUser();
#     sshConnectionConfiguration();
#     cleanNamenode();
    startAllService()
    

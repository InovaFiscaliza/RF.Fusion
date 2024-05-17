<details>
    <summary>Table of Contents</summary>
    <ol>
        <li><a href="#about-appcatalaga">About AppCataloga</a></li>
        <li><a href="#algorithm-overview">Algorithm Overview</a></li>
            <ul>
                <li><a href="#backup-task">Backup task</a></li>
                <li><a href="#processing-task">Processing task</a></li>
                <li><a href="#repository-layout">Repository layout</a></li>
                <li><a href="#list-of-modules-scripts-and-files">List of Modules, Scripts and Files</a></li>
            </ul>
        <li><a href="#setup">Setup</a></li>
            <ul>
                <li><a href="#install-necessary-tools">Install necessary tools</a></li>
                <li><a href="#create-a-system-user">Create a system user</a></li>
                <li><a href="#mount-the-repository">Mount the repository</a></li>
                <li><a href="#install-appcataloga">Install appCataloga</a></li>
                    <ul>
                        <li><a href="#install-python-scripts-and-reference-data">Install python scripts and reference data</a></li>
                        <li><a href="#install-mariadb">Install MariaDB</a></li>
                        <li><a href="#install-python-environment-and-the-required-libraries">Install python environment and the required libraries</a></li>
                    </ul>
            </ul>
        <li><a href="#roadmap">Roadmap</a></li>
        <li><a href="#contributing">Contributing</a></li>
        <li><a href="#license">License</a></li>
    </ol>
</details>

# About appCatalaga

appCataloga is a python script that performs file backup and cataloging from remote hosts to a central repository.

In the context of the RF.Fusion framework, it interfaces with Zabbix scripts that timely post the requests and with files created by agents running on the remote servers.

# Algorithm Overview

For each monitoring station (host) registred in Zabbix, `queryCataloga.py` is called and uses socket to communicate with `appCataloga.py`.

`appCataloga.py` receives the request from `queryCataloga.py` and includes the monitoring station (host) in the backup task queue table in the database.

`appCataloga.py` imediatly respond to `queryCataloga.py` with the backup and processing status for the monitoring station (host), up to that moment, as registered in the database.

Following a request from `queryCataloga.py`, `appCataloga.py` run the `backup_control.py` and `processing_control.py` modules.

`backup_control.py` checks the backup queue for pending backup tasks. If there is a pending backup task, it is executed by the `backup_single_host.py` module.

Several backup tasks may be executed in parallel with multiple `backup_single_host.py` process, up to a limit defined in the configuration file.

`backup_single_host.py` get the list of files to be copied from the index file (`DUE_BACKUP`) and copy the measurement data files to the central repository. Used for linux systems runnng indexerD daemon.

At the end of each backup task, `backup_single_host.py` returns summary information about the backup.

`backup_control.py` get the output from `backup_single_host.py`, create an entry in the processing task queue in the database and update the backup status information for the host.

`processing_control.py` checks the processing queue for pending processing tasks. If there is a pending processing task, it is executed and metadata associated with the measurement data files are extracted and stored in the database. 

## Backup task

Detailed description of the backup task:

* Server access the host using ssh and check the halt flag cookie file (`HALT_FLAG`)
* If halt flag is raised, wait a random time and try again.
* If halt flag is is not lowered after a few tries, raises an error in the backup log and stop.
* When the halt flag is lowered, raising it back and continue the backup process.
* Download the index file ('DUE_BACKUP') and update database
* Copy the measurement data files to the central repository
* Update database with the status of the copied files
* Update file processing queue to extract metadata from copied files and update the measurement database
* Removed the index file in the remote host
* Releases the agent by lowering the halt flag.

## Processing task

Detailed description of the processing task:

* Check the file processing queue for pending tasks
* If there is a pending task, it is executed.
* Extract metadata from the measurement data files
* Update the measurement database with the extracted metadata
* Update the file processing queue with the status of the processed files

## Repository layout

This section of the repository includes the following folders:

* `root` present the folder structure from the appCataloga server from the root folder, with files and folder described in greater detail below.
* `old` present older versions and drafts used for reference. This folder will be removed before the first release.

## List of Modules, Scripts and Files

appCataloga includes several python scripts that perform the following tasks:
| Script module | Description |
| --- | --- |
| /etc/appCataloga/equipmentType.csv | Initial reference data to create the measurement database |
| /etc/appCataloga/fileType.csv | Initial reference data to create the measurement database |
| /etc/appCataloga/measurementUnit.csv | Initial reference data to create the measurement database |
| /etc/appCataloga/IBGE-BR_Municipios_2020_BULKLOAD.csv | Initial reference data to create the measurement database |
| /etc/appCataloga/IBGE-BR_UF_2020_BULKLOAD.csv | Initial reference data to create the measurement database |
| /usr/local/bin/appCataloga/createMeasureDB.sql | Script to create and populate the measurement database |
| /usr/local/bin/appCataloga/createProcessingDB.sql | Script to create the backup and processing task management database |
| /usr/local/bin/appCataloga/environment.yml | Python environment description needed to run all modules. To be used with CONDA |
| /etc/appCataloga/config.py | Constants that define appCataloga behaviour |
| /etc/appCataloga/secret.py | Database user and password information |
| /usr/local/bin/appCataloga/appCataloga.service | Linux service managemente script |
| /usr/local/bin/appCataloga/appCataloga.sh | Shellscript used to start the CONDA envoronment and call appCataloga.py |
| /usr/local/bin/appCataloga/appCataloga.py | appCataloga main module and socket server. See previously described algorithm overview. |
| /usr/local/bin/appCataloga/db_handler.py | Database related classes and functions |
| /usr/local/bin/appCataloga/shared.py | General shared classes and functions |
| /usr/local/bin/appCataloga/backup_control.py | Backup control module. See previously described algorithm overview. |
| /usr/local/bin/appCataloga/backup_single_host.py | Backup data from a single linux host running indexerD daemon. See previously presented backup task description. |
| /usr/local/bin/appCataloga/processing_control.py | Metadata extraction module. See previously presented processing task description. |

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

# Setup

## Install necessary tools

```shell
dnf update

dnf install cifs-utils dos2unix gcc unixODBC.x86_64
```

In case of failue to install or differente platform, you may search the dnf repository for the required packages in the correct version, using the following command

```shell
dnf search unixodbc
```

## Create a system user

The system user will be used to access the repository and to run the application

In the example below, the username is `sentinela`

```shell
useradd -r -s /bin/false sentinela

passwd sentinela

    Changing password for user sentinela.
    New password: <sentinela_pass>
    Retype new password: <sentinela_pass>
    passwd: all authentication tokens updated successfully.
```

Store the credentials in a secure location for later use

Get the ID numbers for the user and group, to be used in the mount command

```shell
id sentinela
```

The UID and GID information will be presented as follows;

```shell
uid=987(sentinela) gid=983(sentinela) groups=983(sentinela)
```

## Mount the repository

Create a credential file for the user that has accces to the repository. It will be different from the user created if a network storage is used

```shell
printf "username=mnt.sfi.sensores.pd\npassword=<PASSWORD>\n" > /root/.reposfi

chmod 600 /root/.reposfi
```

Create a mount point folder and mount the volume using the credential file, UID and GID

```shell
mkdir /mnt/reposfi

mount -t cifs -o credentials=/root/.reposfi,uid=987,gid=983,file_mode=0666,dir_mode=0777 //reposfi/sfi$/SENSORES  /mnt/reposfi
```

One may also yse the following command to mount the volume

```shell
mount -t cifs -o credentials=/root/.reposfi,noperm //reposfi/sfi$/SENSORES  /mnt/reposfi
```

Once the mount is complete with success, one may make it permanent by adding the following line to `/etc/fstab`

```shell
//reposfi/sfi$/SENSORES  /mnt/reposfi  cifs  credentials=/root/.reposfi,uid=987,gid=983,file_mode=0666,dir_mode=0777  0  0

systemctl daemon-reload
```

## Install appCataloga

### Install MariaDB

```shell
dnf module install mariadb
```

Enable MariaDB in SystemCTL

```shell
updatesystemctl enable --now mariadb
```

Secure MariaDB

```shell
mysql_secure_installation

    NOTE: RUNNING ALL PARTS OF THIS SCRIPT IS RECOMMENDED FOR ALL MariaDB
        SERVERS IN PRODUCTION USE!  PLEASE READ EACH STEP CAREFULLY!

    In order to log into MariaDB to secure it, we'll need the current
    password for the root user.  If you've just installed MariaDB, and
    you haven't set the root password yet, the password will be blank,
    so you should just press enter here.

    Enter current password for root (enter for none): 
    OK, successfully used password, moving on...

    Setting the root password ensures that nobody can log into the MariaDB
    root user without the proper authorisation.

    Set root password? [Y/n] Y
    New password: <root_pass>
    Re-enter new password: <root_pass>
    Password updated successfully!
    Reloading privilege tables..
    ... Success!


    By default, a MariaDB installation has an anonymous user, allowing anyone
    to log into MariaDB without having to have a user account created for
    them.  This is intended only for testing, and to make the installation
    go a bit smoother.  You should remove them before moving into a
    production environment.

    Remove anonymous users? [Y/n] n
    ... skipping.

    Normally, root should only be allowed to connect from 'localhost'.  This
    ensures that someone cannot guess at the root password from the network.

    Disallow root login remotely? [Y/n] Y
    ... Success!

    By default, MariaDB comes with a database named 'test' that anyone can
    access.  This is also intended only for testing, and should be removed
    before moving into a production environment.

    Remove test database and access to it? [Y/n] Y
    - Dropping test database...
    ... Success!
    - Removing privileges on test database...
    ... Success!

    Reloading the privilege tables will ensure that all changes made so far
    will take effect immediately.

    Reload privilege tables now? [Y/n] Y
    ... Success!

    Cleaning up...

    All done!  If you've completed all of the above steps, your MariaDB
    installation should now be secure.

    Thanks for using MariaDB!
```

Create user for the application

```shell
mysql -u root -p

    Enter password: 
    Welcome to the MariaDB monitor.  Commands end with ; or \g.
    Your MariaDB connection id is 20
    Server version: 10.3.35-MariaDB MariaDB Server

    Copyright (c) 2000, 2018, Oracle, MariaDB Corporation Ab and others.

    Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.
vd0RpwMLA
MariaDB [(none)]> CREATE USER 'appCataloga'@'localhost' IDENTIFIED BY '<app_pass>';

MariaDB [(none)]> GRANT ALL PRIVILEGES ON BPDATA.* TO 'appCataloga'@'localhost';

MariaDB [(none)]> GRANT ALL PRIVILEGES ON RFDATA.* TO 'appCataloga'@'localhost';

MariaDB [(none)]> FLUSH PRIVILEGES;

MariaDB [(none)]> exit
```

Berfore runnin the deploy script, that wi create the database, ou need to allow the use of sql scripts from the tmp folder

```shell
nano /etc/systemd/system/multi-user.target.wants/mysqld.service
```

Edit the following line

```shell
    # Place temp files in a secure directory, not /tmp
    PrivateTmp=false
```

Use 'CTRL+X' to exit and 'Y' to save the changes

Reload the daemon

```shell
systemctl daemon-reload
systemctl stop mysqld.service
systemctl start mysqld.service
```

Edit the file `/etc/appCataloga/secret.py` to set the database credentials and other essential parameters as described below

```shell
nano /etc/appCataloga/secret.py

#!/usr/bin/env python
"""	Secret used in the appCataloga scripts """
DB_USER_NAME = 'appCataloga' 
DB_PASSWORD = '<app_pass>'
```

Use 'CTRL+X' to exit and 'Y' to save the changes

### Install python scripts and reference data

Get de deployment script to download and copy relevant files to the appropriate folders

```shell
mkdir /tmp/appCataloga

cd /tmp/appCataloga

wget https://raw.githubusercontent.com/InovaFiscaliza/RF.Fusion/main/install/appCataloga/deploy.sh

chmod +x deploy.sh
```

The deployment script have the following options:

* `-h` to show the help message
* `-i` to install the application
* `-u` to update the application
* `-r` to remove the application

To install the application, run the following command

```shell
./deploy.sh -i
```

### Install python environment and the required libraries

Install miniconda under the /usr/local/bin/appCataloga folder

```shell
cd /usr/local/bin/appCataloga

wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh

chmod +x Miniconda3-latest-Linux-x86_64.sh

./Miniconda3-latest-Linux-x86_64.sh

```

The script will present the following output and prompt for the installation location.

Use the following highlighted options:

>Welcome to Miniconda3
>
>...
>
>In order to continue the installation process, please review the license agreement.
>
>Please, press ENTER to continue
>
> \>>> **`ENTER`**
> 
> ...
> 
>Do you accept the license terms? [yes|no]
>
>[no] >>> **`yes`**
>
>Miniconda3 will now be installed into this location: 
>
>/root/miniconda3
>
>\- Press ENTER to confirm the location
>
>\- Press CTRL-C to abort the installation
>
>\- Or specify a different location below
>
>[/root/miniconda3] >>> **`/usr/local/bin/appCataloga/miniconda3`**
>
>Preparing transaction: done
> 
>Executing transaction: done
> no
>installation finished.
>
>Do you wish to update your shell profile to automatically initialize conda?
>...
>
> [no] >>> **`no`**
> 
> ...
> 
> Thank you for installing Miniconda3!

remove Miniconda installation script

```shell
rm -f Miniconda3-latest-Linux-x86_64.sh
```

Activate conda

```shell
source /usr/local/bin/appCataloga/miniconda3/bin/activate
```

create the environment

```shell
(base) conda env create -f /usr/local/bin/appCataloga/environment.yml
```

If you want to test any module, you may activate the environment and run the module directly using:

```shell
conda activate appdata

.\<MODULE>.py
```

To test appCataloga, run the following command

```shell
/usr/local/bin/appCataloga/appCataloga.sh start
```

All relevant information about the execution will be logged in the file `/var/log/appCataloga.log`.

```shell
# /usr/local/bin/appCataloga/appCataloga.sh stop

# cat /var/log/appCataloga.log
20AA/MM/11 hh:mm:ss | p.155869 | Log started
20AA/MM/11 hh:mm:ss | p.155869 | Server is listening on port 5555
20AA/MM/11 hh:mm:st | p.155869 | Signal handler called with signal 15
20AA/MM/11 hh:mm:st | p.155869 | Shutting down....

```

Activate systemctl service that will keep the application running

```shell
systemctl enable --now appCataloga.service
systemctl enable --now appCataloga_host_check
systemctl enable --now appCataloga_file_bkp@0.service 
systemctl enable --now appCataloga_file_bin_proces.service

```

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

# Usage and Maintenance

To backup the database, run the following command

```shell
mysqldump -u root -p --all-databases --result-file=\tmp\appCataloga\databases.sql
```

To restore the database, run the following command

```shell
mysql -u root -p < \tmp\appCataloga\databases.sql
```

To refresh the database entries and reference measurement data, run the following command

```shell
/usr/local/bin/appCataloga/tool_refresh_server.py
```

To refresh the database entries and reference data for a specific node, run the following commands

```shell

/usr/local/bin/appCataloga/tool_refresh_node.py <node_list>
```

where node_list is a comma separated file listing nodes to be checked.

File should include the following columns: <node_id>,<node_UID>,<node_IP>,<remote_user>,<remote_password>

# Roadmap

This section presents a simplified view of the roadmap and knwon issues.

For more details, see the [open issues](https://github.com/FSLobao/RF.Fusion/issues)

* [ ] Rfeye node
  * [x] Station availability status (ICMP)

  
<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- CONTRIBUTING -->
# Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- LICENSE -->
# License

Distributed under the GNU General Public License (GPL), version 3. See [`LICENSE.txt`](../../LICENSE).

For additional information, please check <https://www.gnu.org/licenses/quick-guide-gplv3.html>

This license model was selected with the idea of enabling collaboration of anyone interested in projects listed within this group.

It is in line with the Brazilian Public Software directives, as published at: <https://softwarepublico.gov.br/social/articles/0004/5936/Manual_do_Ofertante_Temporario_04.10.2016.pdf>

Further reading material can be found at:

* <http://copyfree.org/policy/copyleft>
* <https://opensource.stackexchange.com/questions/9805/can-i-license-my-project-with-an-open-source-license-but-disallow-commercial-use>
* <https://opensource.stackexchange.com/questions/21/whats-the-difference-between-permissive-and-copyleft-licenses/42#42>

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- ACKNOWLEDGMENTS -->
## References

* [Conda Cheat Sheet](https://docs.conda.io/projects/conda/en/4.6.0/_downloads/52a95608c49671267e40c689e0bc00ca/conda-cheatsheet.pdf)
* [Readme Template](https://github.com/othneildrew/Best-README-Template)

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

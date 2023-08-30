<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-appcataloga">About AppCataloga</a></li>
    <li><a href="#algorithm-overview">Algorithm Overview</a></li>
    <li><a href="#repository-layout">Repository layout</a></li>
    <li><a href="#setup">Setup</a></li>
    <li><a href="#external_checks">External Checks</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#references">References</a></li>
  </ol>
</details>

# About appCatalaga

appCataloga is a python script that performs file backup and cataloging from remote hosts to a central repository.

In the context of the RF.Fusion framework, it interfaces with Zabbix scripts that timely post the requests and with files created by agents running on the remote servers.

# Algorithm Overview

Agent configuration uses file that provides the essential operational parameters (e.g. [indexerD.cfg](./linux/indexerd/etc/node/indexerD.cfg)), such as:

* Target folder were measurement data is locally stored prior to backup (e.g. `LOCAL_REPO`)
* Working folder were cookies and temporary files are stored (e.g. `SENTINELA_FOLDER`)
* Identification of temporary files and folder (e.g. `TEMP_CHANGED`)
* Identification of the output file, listing the measurement data files to be copied by the server. (e.g. `DUE_BACKUP`)
* Cookie file to signal that file indexing is being performed and no further action should be taken (e.g. `HALT_FLAG`)
* Cookie file to signal the timestamp when the last index task was performed (e.g. `LAST_BACKUP_FLAG`)

Local indexing takes place by listing all files placed within the target folder that were changed since the last indexing was performed.

Server halts the indexing process using the same cookie file. Download the index file and from there, the required measurement data files. The index file is removed at the end and new indexing recommences from a clean file when the server releases the agent.

Server process reports to 

# Repository layout

This section of the repository includes the following folders:

* `automation` folder reference to legacy automation scripts cloned from a private project by Guilherme Braga <<https://github.com/gui1080>>
* `templates` present XML definitions of used measurement templates
* `root` present the folder structure from the zabbix server with in-place external checks used by the described templates

# Setup

Install necessary tools

```shell
dnf update

dnf install cifs-utils
```

Create a system user that will access the repository and get its UID and GID numbers

In the example below, the username is `sentinela`

```shell
useradd -r -s /bin/false sentinela
id sentinela
```

The UID and GID information will be presented as follows;

```shell
uid=987(sentinela) gid=983(sentinela) groups=983(sentinela)
```

Create a credential file for the user that has accces to the repository. It will be different from the user created if a network storage is used

```shell
printf "username=mnt.sfi.sensores.pd\npassword=<PASSWORD>\n" > /root/.reposfi

chmod 600 /root/.reposfi
```

Create a mount point folder and mount the volume using the credential file, UID and GID

```shell
mkdir /mnt/reposfi

mount -t cifs -o credentials=/root/.reposfi,uid=987,gid=983,file_mode=0664,dir_mode=0775 //reposfi/sfi$/SENSORES  /mnt/reposfi
```
One may also yse the following command to mount the volume

```shell
mount -t cifs -o credentials=/root/.reposfi,noperm //reposfi/sfi$/SENSORES  /mnt/reposfi
```


Once the mount is complete with success, one may make it permanent by adding the following line to `/etc/fstab`

```shell
//reposfi/sfi$/SENSORES  /mnt/reposfi  cifs  credentials=/root/.reposfi,uid=987,gid=983,file_mode=0664,dir_mode=0775  0  0

systemctl daemon-reload
```


Install MariaDB.

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

Create database and user for the application

```shell
mysql -u root -p

    Enter password: 
    Welcome to the MariaDB monitor.  Commands end with ; or \g.
    Your MariaDB connection id is 20
    Server version: 10.3.35-MariaDB MariaDB Server

    Copyright (c) 2000, 2018, Oracle, MariaDB Corporation Ab and others.

    Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

MariaDB [(none)]> SOURCE /usr/local/bin/createMeasureDB.sql

MariaDB [(none)]> SOURCE /usr/local/bin/createProcessingDB.sql

MariaDB [(none)]> CREATE USER 'appCataloga'@'localhost' IDENTIFIED BY '<app_pass>';

MariaDB [(none)]> GRANT ALL PRIVILEGES ON BPDATA.* TO 'appCataloga'@'localhost';

MariaDB [(none)]> GRANT ALL PRIVILEGES ON RFDATA.* TO 'appCataloga'@'localhost';

MariaDB [(none)]> FLUSH PRIVILEGES;
```

After the database is created you may remove the original csv files to free up space

```shell
rm -f /etc/appCataloga/*.csv
```

Install gcc dependencies related to python scripts

```shell
dnf install gcc
```

Search and Install ODBC driver for Python

```shell
dnf search unixodbc

dnf install unixODBC.x86_64
```

Install python and the required associated libraries.

It is suggested the use of [conda](https://docs.conda.io/) (or [mamba](https://mamba.readthedocs.io/en/latest/)) as environment manager and, as [conventional](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html), the environment is controlled by the [`environment.yml`](./root/usr/local/bin/environment.yml) file.

The [`environment.yml`](./root/usr/local/bin/environment.yml) file is where you specify any packages available on the [Anaconda repository](https://anaconda.org) as well as from the Anaconda Cloud (including [conda-forge](https://conda-forge.org)) to install for your project. 

To (re)create the environment on your installation of [conda](https://conda.io) via [anaconda](https://docs.anaconda.com/anaconda/install/), [miniconda](https://docs.conda.io/projects/continuumio-conda/en/latest/user-guide/install/) or preferably [miniforge](https://github.com/conda-forge/miniforge), you only need to pass the `environment.yml` file, which will install requirements and guarantee that whoever uses your code has the necessary packages (and correct versions).

```shell
conda env create -n appdata -f environment.yml
```

Activate systemctl service that will keep the application running


<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

# Modules, Scripts and Files

appCataloga includes several python scripts that perform the following tasks:
| Script module | Description |
| --- | --- |
| `appCataloga.py` | Main script that performs the following tasks: <ul><li>Reads the configuration file</li><li>Reads the index file</li><li>Reads the cookie file</li><li>Reads the list of files to be copied</li><li>Reads the list of files to be deleted</li><li>Reads the list of files to be updated</li><li>Reads the list of files to be renamed</li><li>Reads the list of files to be moved</li><li>Reads the list of files to be created</li><li>Reads the list
| `CreateDatabase_mysqk.sql` | Script that creates the database and tables used by the application in SQL compatible with MariaDB V10.3 |
| `CreateDatabase_sqlserver.sql` | Script that creates the database and tables used by the application in SQL compatible with Microsoft SQL Server 2019 |
| `CRFSbinHandler.py` | Script that handles the CRFS binary files |
| `dbHandler.py` | Script that handles the database |
| `root/etc/appCataloga/*.csv` | Set of files containing initial reference data to be loaded into the database |
| `root/etc/appCataloga/.credentials.py` | File containing the credentials to access the database |
| `root/root/.reposfi` | File containing the credentials to access the repository |

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>


# Algorithm Overview

For each monitoring station (host) registred in Zabbix, queryCataloga.py is called and uses socket to communicate with appCataloga.

appCataloga receives the request from queryCataloga.py and includes the monitoring station (host) in the backup task queue.

appCataloga respond to queryCataloga.py with the backup status for the monitoring station (host).

For the backup server, also registred in Zabbix as a host, queryCataloga.py is called and uses socket to communicate with runBackup.py.

runBackup receives the request from queryCataloga.py and check task queue for pending backup tasks. If there is a pending task, it is executed.

runBackup respond to queryCataloga.py with the status of the backup queue.

the backup task is executed according to the following steps:

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

the file processing task is executed according to the following steps:
* Check the file processing queue for pending tasks
* If there is a pending task, it is executed.
* Extract metadata from the measurement data files
* Update the measurement database with the extracted metadata
* Update the file processing queue with the status of the processed files


<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

# Roadmap

This section presents a simplified view of the roadmap and knwon issues.

For more details, see the [open issues](https://github.com/FSLobao/RF.Fusion/issues)

* [ ] Rfeye node
  * [x] Station availability status (ICMP)

  
<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- LICENSE -->
## License

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

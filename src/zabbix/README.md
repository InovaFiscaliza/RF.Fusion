<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-zabbix-source">About Zabbix Source</a></li>
    <li><a href="#repository-layout">Repository Layout</a></li>
    <li><a href="#setup">Setup</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#references">References</a></li>
  </ol>
</details>

# About Zabbix Source

Zabbix is used in the appCatalog to maintain the reference data about spectrum monitoring stations, including updated fix site locations and logical address to access each station.

Source data for zabbix is provided in the form of:

* [templates](https://www.zabbix.com/documentation/current/en/manual/config/templates), definining itens, triggers, graphs and other artifacts that control how zabbix will monitor the existing hosts.
* [external checks](https://www.zabbix.com/documentation/current/en/manual/config/items/itemtypes/external), intended to be used to capture and process specific information from hosts, when is not practical to use standard item types.

Zabbix documentation can be found at: <https://www.zabbix.com/documentation/current/en/manual>

The rationale behind the use of Zabbix is to provide a full feature tool for the day-to-day business of managing operations and maintenance for the spectrum monitoring network and, using the same database, automate file transfers and integration process that maintain the data flow from stations to central servers.

Initial attempts of using simple tools like [rsync](https://linux.die.net/man/1/rsync) and similar tools to backup data produced by the monitoring stations failed due to the lack of support to the maintenance of the infrastructure, limitations in file handling capabilities and the services needed to maintain a central  station database.

when operating the monitoring network, many problems arises that may create problems to the data collection and processing, including changes in IP, location and other issues. Thus retrieving a large set of metadata by the network monitoring tool enable a more reliable automation of the data gathering process.

# Repository layout

This section of the repository includes the following vase folders:

* `automation` folder reference to legacy automation scripts cloned from a private project by Guilherme Braga <<https://github.com/gui1080>>
* `templates` present XML definitions of used measurement templates
* `root` present the folder structure from the zabbix server with in-place external scripts used by the described templates

# Setup

Templates can be loaded directly from Zabbix web interface.

Currently, only the CWRMU template uses external scripts

External scripts should be stored in the defined folder and properties should be setup to reduce the associated security risks, e.g.

```shell
chmod 700 queryDigitizer.py

chown zabbix queryDigitizer.py

chgrp zabbix queryDigitizer.py
```

Existing python script uses only standard libraries, thus, no special setup is required.

In the event that more sophisticated processing is required, the use of environments and additional setup may be required.

<p align="right"; font="bold"; color="green">Notebook were tested with [Python 3.11 ](https://www.python.org).

# Roadmap

This section presents a simplified view of the roadmap and knwon issues.

For more details, see the [open issues](https://github.com/FSLobao/RF.Fuse/issues) 

* [ ] Rfeye node <[rfeye_node_template.json](./templates/rfeye_node_template.json)>
  * [x] Station availability status (ICMP)
  * [ ] Station id
  * [x] Network data
  * [x] Processor and storage data
  * [x] Environment data
  * [x] GPS data
  * [ ] Autonomous monitoring status (logger/script)
  * [ ] Remote operation status (rfeye site 9999)
  * [ ] Monitoring alert status (mask broken alert)
* [ ] CW RMU <[CWRMU_template.json](./templates/CWRMU_template.json)>
  * [x] Station availability status (ICMP)
  * [ ] Station id
    * [x] digitizer serial number
  * [x] Network data
  * [ ] Processor and storage data
  * [x] Environment data
  * [x] GPS data
  * [ ] Autonomous monitoring status (cwsm script)
  * [x] Remote operation status (cst status)
  * [ ] Monitoring alert status (mask broken alert)
* [ ] UMS300
  * [x] Station availability status (ICMP)
  * [ ] Station id
  * [ ] Network data
  * [ ] Processor and storage data
  * [ ] Environment data
  * [ ] GPS data
  * [ ] Autonomous monitoring status (argus script)
  * [ ] Remote operation status (argus ports)
  * [ ] Monitoring alert status (mask broken alert)
* [ ] ERMx
  * [x] Station availability status (ICMP)
  * [ ] Station id
  * [ ] Network data
  * [ ] Processor and storage data
  * [ ] Environment data
    * [x] Volt Smartweb
  * [ ] GPS data
  * [ ] Autonomous monitoring status (appColeta script)
  * [ ] Operation status (appColeta stream)
  * [ ] Monitoring alert status (appColeta alert)
* [ ] VPN Server (OpenVPN)
  * [x] Server availability status (ICMP)
  * [x] Server resources (CPU, Memory, Process load, Storage)
  * [ ] Openvpn log
* [ ] Monitor and Automation (Zabbix)
  * [x] Server availability status (ICMP)
  * [x] Server resources (CPU, Memory, Process load, Storage)
  * [ ] Server 
* [ ] Publish (Landel)
  * [ ] Server availability status (ICMP)
  * [ ] Server resources (CPU, Memory, Process load, Storage)
  * [ ] Data catalog processing status
* [ ] Data Storage (RepoSFI)
  * [ ] Storage availability status (ICMP)
  * [ ] Available/used space in live area
  * [ ] Available/used space in offload area
  * [ ] File area transfer automation process status
* [ ] Data Analytics (appWeb)
  * [ ] Server resources (CPU, Memory, Process load, Storage)
  * [ ] Matlab web server status
  

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


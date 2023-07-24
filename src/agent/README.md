<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-zabbix-source">About Zabbix Source</a></li>
    <li><a href="#repository-layout">Repository Layout</a></li>
    <li><a href="#setup">Setup</a></li>
    <li><a href="#external_checks">External Checks</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#references">References</a></li>
  </ol>
</details>

# About Agents

Agents perform file indexing in the remote monitoring stations, preparing then for copy into the repository.

Two agent versions were created, for linux and windows systems

Windows version include metadata extraction that is not present in the linux version due to differences in the capabilities of the target equipment.

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

This section of the repository includes the following vase folders:

* `automation` folder reference to legacy automation scripts cloned from a private project by Guilherme Braga <<https://github.com/gui1080>>
* `templates` present XML definitions of used measurement templates
* `root` present the folder structure from the zabbix server with in-place external checks used by the described templates

# Setup

Templates can be loaded directly from Zabbix web interface.

Some of the templates will require external script that are provided under the root folder.

External scripts should be stored in the defined folder and properties should be setup to reduce the associated security risks, e.g., for the queryDigitizer.py script one must run the following commands in the shell CLI and with SU privileges.

```shell
chmod 700 queryDigitizer.py

chown zabbix queryDigitizer.py

chgrp zabbix queryDigitizer.py
```

Existing python script uses only standard libraries, thus, no special setup is required.

In the event that more sophisticated processing is required, the use of environments and additional setup may be required.

# External Checks

| External Check | Description |
| --- | --- |
| `queryDigitizer.py` | Perform VISA SCPI query of the CW RMU digitizer in order to acquire information about the receiver identification, environmental and location data. |
| `queryLoggerUDP.py` | Perform UDP query of rfeye logger stream created according to the [rfeye logger example script](/test/logger/README.md) |
| `rfeyeIPname.py` | Change host IP according to the host identification obtained via Mac Address. Uses the `http://<IP>/cgi-bin/ifconfig.cgi` to obtain the MAC in the format: `HWaddr 00:1e:89:00:MN:OP` where the last digits (`MN:OP`) corresponds to the equipment serial for the station `rfeye00MNOP`. If using fixed IP and hostname does not match the current host, change IP from the two hosts (identified and configured). If using DNS, generate alert for VPN key misplacement |
| `dnsIPswitch.py` | Change the zabbix active host direction from IP to DNS and vice versa in case there is an IP configured and the current configuration fail to respond within X retries |
| `siteData.py` | Update group and tag information for the host based on configured sites and reverse geolocation data using OSM nominatin|

# Roadmap

This section presents a simplified view of the roadmap and knwon issues.

For more details, see the [open issues](https://github.com/FSLobao/RF.Fusion/issues)

* [ ] Rfeye node
  * [x] Station availability status (ICMP)
  * [ ] Station id
  * [x] Network data
  * [x] Processor and storage data
  * [x] Environment data
  * [x] GPS data
  * [x] Autonomous monitoring status (logger/script)
  * [ ] Remote operation status (rfeye site 9999)
  * [x] Monitoring alert status (mask broken alert)
* [ ] CW RMU
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

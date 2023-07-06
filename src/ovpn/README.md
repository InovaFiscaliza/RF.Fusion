<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-openvpn-source">About OpenVPN Source</a></li>
    <li><a href="#repository-layout">Repository Layout</a></li>
    <li><a href="#setup">Setup</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#references">References</a></li>
  </ol>
</details>

# About OpenVPN Source

OpenVPN Community Edition is used in the appCatalog to provide connectivity and a security layer to protect the monitoring stations.

Source data for OpenVPN is provided in the form of [various scripts](./root/etc/openvpn/easy-rsa/) that further automate easy-rsa functions for large network deployment.

Additionally, a [script](./root/usr/lib/openvpn/ovpn_monitor.py) is provided to parse and publish selected OpenVPN log data, providing information to the network monitoring service provided by Zabbix with reduced processing requirements by the zabbix server, comparing to alternatives of enabling a direct connection to the OpenVPN management interface or using Zabbix agent to directly parse OpenVPN logs.

Configuration files are provided as example but are stripped from private data, including, keys, used IPs and contact information. A tag in the format `<DEFINE ...>` is used to mark such replacements.


# Repository layout

This section of the repository includes the following folders:

| Script/File                                       | Use |
| ------------------------------------------------- | -- |
| `/etc/openvpn/easy-rsa/deploy`                    | Script that deploys all other scripts and configuration files listed below. Should be run after openvpn installation  |
| `/etc/openvpn/easy-rsa/pki/vars`                  | pre-configured vars file as discussed below |
| `/etc/openvpn/easy-rsa/kkreate`                   | Create a single client package with multiple files in tgz format |
| `/etc/openvpn/easy-rsa/ukkreate`                  | Create a single client package with a single file in unified .ovpn format |
| `/etc/openvpn/easy-rsa/batch_kk`                  | Create clients in batch based in the list configured in the specified conf file as per example rfeye.conf |
| `/etc/openvpn/easy-rsa/batch_kk`                  | Create clients in batch using the tgz package format based in the list configured in the specified conf file as per example rfeye.conf |
| `/etc/openvpn/easy-rsa/batch_ukk`                 | Create clients in batch using the unified format based in the list configured in the specified conf file as per example ums.conf |
| `/etc/openvpn/easy-rsa/build_ccd`                 | Build de client specific configuration files with assigned fixed IP to each client |
| `/etc/openvpn/easy-rsa/cwsm.conf` | Configuration file used by build_ccd and batch_kk to create ccd and keys in batch for CWSM RMU stations |
| `/etc/openvpn/easy-rsa/ermx.conf` | Configuration file used by build_ccd and batch_kk to create ccd and keys in batch for ERMx stations |
| `/etc/openvpn/easy-rsa/miaer.conf` | Configuration file used by build_ccd and batch_kk to create ccd and keys in batch for MIAer stations |
| `/etc/openvpn/easy-rsa/rfeye.conf` | Configuration file used by build_ccd and batch_kk to create ccd and keys in batch for RFEye Node stations |
| `/etc/openvpn/easy-rsa/ums.conf` | Configuration file used by build_ccd and batch_kk to create ccd and keys in batch for UMS300 stations |
| `/etc/openvpn/easy-rsa/rme_server.conf` |Configuration file used by build_ccd and batch_kk to create ccd and keys in batch for RME Server network |
| `/etc/openvpn/server/server.conf`                 | Server configuration file |
| `/etc/openvpn/easy-rsa/client_script/client.conf` | Template for client configuration file. Tag `<client>` will be replaced by the client name from the used configuratio file when building the package by batch_kk or batch_ukk|
| `/etc/openvpn/easy-rsa/client_script/up.sh`       | Script to be run on linux clients when connection goes up |
| `/etc/openvpn/easy-rsa/client_script/down.sh`     | Script to be run on linux clients when connection goes down |
| `/usr/lib/openvpn/ovpn_monitor.py`     | Script that provide integration with Zabbix and the DNS server |


# Setup

Please refer to the [OpenVPN Documentation](../../docs/ovpn/README.md) within this project for more details about installation and deployment of [OpenVPN](https://openvpn.net/community/) and [Easy-RSA](https://github.com/OpenVPN/easy-rsa).

<p align="right"; font="bold"; color="green">Notebook were tested with [Python 3.11 ](https://www.python.org).

# Roadmap

This section presents a simplified view of the roadmap and knwon issues.

For more details, see the [open issues](https://github.com/FSLobao/RF.Fuse/issues) 

* [x] Server setup
* [x] OpenVPN Install
* [x] Test compatibility of updated OpenVPN configuration
* [ ] Automation script
  * [x] build script for single host key and configuration package (multiple files in tgz format)
  * [x] build script for single host key and configuration package (single files (unified) in ovpn format)
  * [x] build script for multiple host keys and configuration packages (tgz format)
  * [x] build script for multiple host keys and configuration packages (ovpn format)
  * [x] build script for ccd file creation
* [ ] OpenVPN Optimization
  * [x] Test optimum configuration to increase performance
  * [ ] Update server configuration for default optimum perfornance
* [ ] Zabbix/DNS integration for OpenVPN Service
  * [ ] [Configure logrotate and timestamp](https://medium.com/@Dylan.Wang/
  * [ ] Test the use of Zabbix_sender for zabbix integration [zabbix_sender](https://www.zabbix.com/documentation/6.4/en/manpages/zabbix_sender?hl=zabbix_sender) and [agent](https://www.zabbix.com/documentation/current/en/manual/config/items/itemtypes/log_items) alternatives
  * [ ] Parse openvpn_log file
  * [ ] Zabbix integration
  * [ ] DNS Integration

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

* About [OpenVPN](https://openvpn.net/community/)
* About [Easy-RSA](https://github.com/OpenVPN/easy-rsa)

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>


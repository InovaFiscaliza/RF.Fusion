# About Zabbix Notebooks

Zabbix is used in the appCatalog to maintain the reference data about spectrum monitoring stations, including updated fix site locations and logical address to access each station.

This notebook series was created to perform semi-automatic cleaning of Zabbix database by interacting with files for mass data review and update.

Simple updates, when the desired group of host can be fileterd by existing Zabbix classification, can be easiely updated using Zabbix mass update web interface.

The core python library used for Zabbix interface is: <https://github.com/lukecyca/pyzabbix>

Zabbix API documentation can be found at: <https://www.zabbix.com/documentation/current/en/manual/api/reference>

# Zabbix host definition

Zabbix is very flexible and the use can be adapted according to user needs.

For the purpose of the current project, the following data was used

* HOST
  * GROUP, classification metadata data associated to several hosts, usually ten or more hosts
    * `Type` (Tipo Equipamento) e.g. link, station
    * `Status` (Situação), e.g. active, nomadic, defective.Class varies according to host type (station or link)
    * `State` (UF)
    * `Contract` (Contrato), mostly associated with links
    * `Link Technology` (tecnologia de acesso), e.g satelite, fiber, terrestrial wireless link
    * `Equipment Model` (Modelo da estação), e.g. rfeye_node, cwsm, ums300, miaer
  * TAG, classification metadata data is associated with few hosts, usually less then 4 hosts.
    * `County` (Município)
    * `Site` (Local), individual identification of the instalation site, e.g airport tower
    * `owner` (Detentor), administrative unit responsible for the asset
    * `test` (Teste), special group for test purposes. All other groups and tags should be removed in order to avoid presenting the host on the grafana interface
    * `legacy` (ovpn), special group for legacy hosts. All other groups and tags should be removed in order to avoid presenting the host on the grafana interface.
  * INVENTORY, metadata associated with the identification of single hosts with some replication fo group information for completeness.
    * `location_lat` geografic latitude coordinate in degrees. Used to plot the host on the map by Zabbix and Grafana
    * `location_lon` geografic coordinate  in degrees
    * `contract_number` same as group Contract
    * `site_city` same as tag county
    * `site_state` same as group state
    * `alias` same as tag Local
    * `asset_tag`, id key for administrative asset database reference number, e.g. número SIADS 
    * `tag`, hyperlink to asset to the equipment control database, e.g. [#56821]('https://sistemas.anatel.gov.br/fiscaliza/issues/56821)
    * `type`, same as group type
    * `model`, same asgroup Equipment Model
    * `vendor`, equipment manufacturer.
  * `Name`. Name should be simple reference, without spaces for compatibility issues and commonly used as reference to the equipment itself, not the place where it is operating
  * `IP`. Functional IP that may be used as alternative to the DNS. Should an IP that can be directly accessed by the Zabbix server, either public or within the same private network.
  * `DNS`. Functional name that can be resolved by the DNS server available to the Zabbix server.

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

# Setup

Suggested the use of [conda](https://docs.conda.io/) (or [mamba](https://mamba.readthedocs.io/en/latest/)) as environment manager and, as [conventional](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html), the environment is controlled by the `environment.yml` file.

The `environment.yml` file is where you specify any packages available on the [Anaconda repository](https://anaconda.org) as well as from the Anaconda Cloud (including [conda-forge](https://conda-forge.org)) to install for your project. Ensure to include the pinned version of packages required by your project (including by Jupyter notebooks).

To (re)create the environment on your installation of [conda](https://conda.io) via [anaconda](https://docs.anaconda.com/anaconda/install/), [miniconda](https://docs.conda.io/projects/continuumio-conda/en/latest/user-guide/install/) or preferably [miniforge](https://github.com/conda-forge/miniforge), you only need to pass the `environment.yml` file, which will install requirements and guarantee that whoever uses your code has the necessary packages (and correct versions). 

<p align="right"; font="bold"; color="green">Notebook were tested with [Python 3.11 ](https://www.python.org).

```
conda env create -n zbx -f environment.yml
```

See also: [Conda Managing Environments](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)

<p align="right"; font="bold"; color="green">(<a href="#indexerd-md-top">back to top</a>)</p>

# Usage

Notebooks were created using MS VSCode. `settings.json` and `launch.json` are intended to be used with this editor.

Additional setup of the environment defined in `settings.json` may be required.

`zbxHostGroup.csv` and `zbxHostInterface.csv` are provided as examples of files created by the notebooks for backup and export of data from zabbix hosts, interfaces and groups in CSV format.

`ZabbixClean.xlsx` is provided as an example of input file to be used for mass update of zabbix host and interface data.

<p align="right"; font="bold"; color="green">(<a href="#indexerd-md-top">back to top</a>)</p>

# Roadmap

This section presents a simplified view of the roadmap and knwon issues.

For more details, see the [open issues](https://github.com/FSLobao/RF.Fuse/issues) 

* [x] Clean link data
* [x] Clean rfeye node data
* [ ] Clean UMS300 data
  * [x] create UMS300 host
  * [ ] update inventory, tags and groups
* [ ] Clean CWSM data
  * [x] create CWSM host
  * [ ] update inventory, tags and groups


<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- LICENSE -->
## License

Distributed under the GNU General Public License (GPL), version 3. See [`LICENSE.txt`](../../LICENSE) for more information.

For additional information, please check <https://www.gnu.org/licenses/quick-guide-gplv3.html>

This license model was selected with the idea of enabling collaboration of anyone interested in projects listed within this group.

It is in line with the Brazilian Public Software directives, as published at: <https://softwarepublico.gov.br/social/articles/0004/5936/Manual_do_Ofertante_Temporario_04.10.2016.pdf>

Further reading material can be found at:
* <http://copyfree.org/policy/copyleft>
* <https://opensource.stackexchange.com/questions/9805/can-i-license-my-project-with-an-open-source-license-but-disallow-commercial-use>
* <https://opensource.stackexchange.com/questions/21/whats-the-difference-between-permissive-and-copyleft-licenses/42#42>

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* [Conda Cheat Sheet](https://docs.conda.io/projects/conda/en/4.6.0/_downloads/52a95608c49671267e40c689e0bc00ca/conda-cheatsheet.pdf)
* [Readme Template](https://github.com/othneildrew/Best-README-Template)

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>


<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-zabbix-source">About Zabbix Source</a></li>
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
[al](#algorithm-overview)
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

Local indexing takes place by listing all files placed within the target folder that were changed since the last indexing was performed. (`LAST_BACKUP_FLAG`)

The `LAST_BACKUP_FLAG` time is updated at the end of the indexing process, such as the current index file ('DUE_BACKUP') reflects the file list up to that moment.

The agent runs periodically (cron job) to perform this task and signal the pocessing status using cookie files (`HALT_FLAG`).

Server may halt the indexing process using the same cookie file.

Server download the index file ('DUE_BACKUP') and from there, the required measurement data files.

The index file is removed at the end and new indexing recommences from a clean file when the server releases the agent.

# Repository layout

This section of the repository includes the following vase folders:

* `linux\indexerd` folder with agent source for debian linux systems, structured as to create a .deb instalation package
* `linux\AnatelUpgradePack_Node_20-6_v1` folder with agent source for debian linux systems, structured as to create a .deb instalation package
* `windows` folder store the equivalent agent created to run in windows systems

# Setup

On CRFS nodes, the instalation can be run using the standard update feature or running the following command:

```sh
dpkg-deb -i indexerD
```

To build the instalation package from source, initially make sure that the CHMOD is properly set for the execution scritps after download.

Once set, run the following commands at the local RF.Fusion/src/agent/linux folder:

```sh
dpkg-deb --build indexerD
```



# Roadmap

This section presents a simplified view of the roadmap and knwon issues.

For more details, see the [open issues](https://github.com/FSLobao/RF.Fusion/issues)

* [ ] :Linux indexerd
  * [x] code agent
  * [ ] code upgrade.sh to create cron job
  * [ ] test crfs agent
* [ ] Windows indexerd
  * [ ] code agent
  
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

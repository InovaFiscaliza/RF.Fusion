<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a name="indexerd-md-top"></a>

<!-- PROJECT SHIELDS -->
<!--
*** based on https://github.com/othneildrew/Best-README-Template
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-RF.Fuse">About RF.Fuse</a></li>
    <li><a href="#background">Background</a></li>
      <ul>
        <li><a href="#monitoring-unit">Monitoring Unit</a></li>
        <li><a href="#network-server-core">Network Server Core</a></li>
      </ul>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->
# About RF.Fuse

RF.Fuse is a set of network integration and data backup components for a Spectrum Monitoring Network.

Although conceived with such network in mind, most of the modules could be adapted to suit other needs associated with data collection from any distributed automated sensor network.

Modules were constructed with the idea of maximizing code reuse by employing standard open source tools to perform core tasks and specific modules to perform specific equipment and data integrations

# Background

To better understand modules in this repository is essential to understand the general architecture of a spectrum monitoring network, such as presented in the following figure.

![General Diagram for the Spectrum Monitoring Network](./docs/images/general_diagram.svg)

The elements in the above diagram may be briefily described as follows:

## Monitoring Unit

The network itself can be composed by several monitoring units, up to a few hundred.

Each monitoring unit integrates a series of functional components as follows

- <span style="color:#2E549780">**Antenna & Receiver:**</span> Is the data acquisition front-end, from the RF receiving antennas to the digitizer and DSP that provides data streams with IF IQ data, spectrum sweeps and demodulated data. Each equipment may provide a different set of data.
- <span style="color:#2E549780">**Processor:**</span> Is a generic data processor running linux or windows. It's the local brain of the monitoring station, responsible to perform data requests to the acquisition front-end, any additional processing for data analysis and tagging, finally, manage the local data repository.
- <span style="color:#2E549780">**Environment Control:**</span> may be composed of several elements that are accessory to the measurement, such as temperature control, security detectors and cameras, UPS and power supply management, etc.
- <span style="color:#2E549780">**Router, Firewall and Network Interfaces:**</span> may be composed of several elements that interconnect elements within the station and from it to the outside world. Common solutions provide up to 3 interfaces including an ethernet cable, an integrated 4G or 5G modem to connect to to the mobile WAN network and a VPN connection, that allows for a secure communication with the server core

Monitoring units used with RF.Fuse include [CRFS RFeye Node 20-6](https://www.crfs.com/product/receivers/rfeye-node-20-6/); [Celplam CWRMU](https://www.celplan.com/products/test-measurement/cellwirelesssm/); [Rohde&Schwarz UMS300](https://www.rohde-schwarz.com/es/productos/sector-aeroespacial-defensa-seguridad/aplicacion-en-el-exterior/rs-ums300-compact-monitoring-and-location-system_63493-56146.html) and further units integrates by the use of appColeta using VISA/SCPI to access data from spectrum analysers and monitoring receivers from various manufacturers.

## Network Server Core

Composed by a series of functional components as follows

- <span style="color:#6DAC4680">**VPN Server:**</span> Provide secure connection and network integration between the monitoring units and the network core servers
- <span style="color:#6DAC4680">**Monitor and Automation:**</span> Run services responsible for monitoring the health of the monitoring units, essential network services. Orchestrate the data backup from the monitoring units to the core server data storage. Employs Zabbix and Grafana as core applications
- <span style="color:#6DAC4680">**Publish:**</span> Run services responsible catalog and publish data for direct user consumption. Employs nginx as a core and additional
- <span style="color:#6DAC4680">**Data Storage:**</span> Network storage attached to the server core. Provide a shared file space to receive data from the monitoring units, share with users through the publication service and data analytics services.
- <span style="color:#6DAC4680">**Data Analytics:**</span> Rum services related to the data analysis, either autonomous processing and with user interfaces.

# RF.Fuse Bricks and Blocks

## OpenVPN Integration

`README.MD`
`ovpn_monitor.py`: Provide data from the OpenVPN management service as a JSON dataset that can be accessed by clients using http to receive recent information about the VPN service.



## Zabbix Integration

`dns_ip_switch`:


- VPN Server, required to enable the network integration and sensor security employs OpenVPN
- Monitoring and basic automation employs Zabbix and Grafana as core applications
- Data publication employs nginx as a core, enabling users to download data through http service for desktop processing
- Data storage employs standard CIFS file sharing between servers
- Data analytics is provided by appAnalise and future corresponding webApp.



<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- GETTING STARTED -->
## Getting Started

.

### Prerequisites

This is an example of how to list things you need to use the software and how to install them.

* npm

  ```sh
  npm install npm@latest -g
  ```

#### Dependencies

The next step is ensure your code is maintainable, realiable and reproducible by including
any dependencies and requirements, such as packages, configurations, secrets (template) and addtional instructions.

The <span style="color:#3EACAD">template</span> suggests to use [conda](https://docs.conda.io/) (or [mamba](https://mamba.readthedocs.io/en/latest/)) as environment manager and, as [conventional](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html), the environment is controlled by the `environment.yml` file.

The `environment.yml` file is where you specify any packages available on the [Anaconda repository](https://anaconda.org) as well as from the Anaconda Cloud (including [conda-forge](https://conda-forge.org)) to install for your project. Ensure to include the pinned version of packages required by your project (including by Jupyter notebooks).

```
channels:
  - conda-forge
  - defaults
dependencies:
  - python=3.9
  - bokeh=2.4.3
  - pandas=1.4.3
  - pip:
    - requests==2.28.1
```

To (re)create the environment on your installation of [conda](https://conda.io) via [anaconda](https://docs.anaconda.com/anaconda/install/), [miniconda](https://docs.conda.io/projects/continuumio-conda/en/latest/user-guide/install/) or preferably [miniforge](https://github.com/conda-forge/miniforge), you only need to pass the `environment.yml` file, which will install requirements and guarantee that whoever uses your code has the necessary packages (and correct versions). By default, the <span style="color:#3EACAD">template</span> uses [Python 3.9](https://www.python.org).

```
conda env create -n <your-environment-name> -f environment.yml
```

In case your project uses Python, it is _strongly_ recommended to distribute it as a [package](https://packaging.python.org/).

```{important}
The <span style="color:#3EACAD">template</span> contains an example - the [datalab](https://github.com/worldbank/template/tree/main/src/datalab) Python package - and will automatically find and install any `src` packages as long as `pyproject.yml` is kept up-to-date.
```

```{seealso}
[Conda Managing Environments](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
```


### Installation

_Below is an example of how you can instruct your audience on installing and setting up your app. This template doesn't rely on any external dependencies or services._

1. Get a free API Key at [https://example.com](https://example.com)
2. Clone the repo

   ```sh
   git clone https://github.com/your_username_/Project-Name.git
   ```

3. Install NPM packages

   ```sh
   npm install
   ```

4. Enter your API in `config.js`

   ```js
   const API_KEY = 'ENTER YOUR API';
   ```

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>


#### Jupyter Notebooks

[Jupyter Notebooks](https://jupyter.org) can be beautifully rendered and downloaded from your book. By default, the <span style="color:#3EACAD">template</span> will render any files listed on the [table of contents](#table-of-contents) that have a notebook structure. The <span style="color:#3EACAD">template</span> comes with a Jupyter notebook example, `notebooks/world-bank-api.ipynb`, to illustrate.

```{important}
Optionally, [Jupyter Book](https://jupyterbook.org) can execute notebooks during the build (on GitHub) and display **code outputs** and **interactive visualizations** as part of the *documentation* on the fly. In this case, Jupyter notebooks will be executed by [GitHub Actions](https://github.com/features/actions) during build on each commit to the `main` branch. Thus, it is important to include all [requirements and dependencies](#dependencies) in the repository. In case you would like to ignore a notebook, you can [exclude files from execution](https://jupyterbook.org/en/stable/content/execute.html#exclude-files-from-execution).
```

```{seealso}
[Jupyter Book Write executable content](https://jupyterbook.org/en/stable/content/executable/index.html)
```

<!-- USAGE EXAMPLES -->
## Usage

Use this space to show useful examples of how a project can be used. Additional screenshots, code examples and demos work well in this space. You may also link to more resources.

_For more examples, please refer to the [Documentation](https://example.com)_

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- ROADMAP -->
## Roadmap

* [x] Add Changelog
* [x] Add back to top links
* [ ] Add Additional Templates w/ Examples
* [ ] Add "components" document to easily copy & paste sections of the readme
* [ ] Multi-language Support
  * [ ] Chinese
  * [ ] Spanish

See the [open issues](https://github.com/othneildrew/Best-README-Template/issues) for a full list of proposed features (and known issues).

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- CONTACT -->
## Contact

Your Name - [@your_twitter](https://twitter.com/your_username) - <email@example.com>

Project Link: [https://github.com/your_username/repo_name](https://github.com/your_username/repo_name)

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

Use this space to list resources you find helpful and would like to give credit to. I've included a few of my favorites to kick things off!

* [Choose an Open Source License](https://choosealicense.com)
* [GitHub Emoji Cheat Sheet](https://www.webpagefx.com/tools/emoji-cheat-sheet)
* [Malven's Flexbox Cheatsheet](https://flexbox.malven.co/)
* [Malven's Grid Cheatsheet](https://grid.malven.co/)
* [Img Shields](https://shields.io)
* [GitHub Pages](https://pages.github.com)
* [Font Awesome](https://fontawesome.com)
* [React Icons](https://react-icons.github.io/react-icons/search)

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[smn_overview]: https://github.com/FSLobao/RF.Fuse/tree/main/docs/images/general_diagram.svg


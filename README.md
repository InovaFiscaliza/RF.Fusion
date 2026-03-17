[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/InovaFiscaliza/RF.Fusion)

# RF.Fusion

RF.Fusion is a spectrum-monitoring integration platform centered on three
practical concerns:

- collecting and organizing files from remote monitoring stations
- cataloging spectra and repository metadata
- exposing operational and analytical views through a lightweight web UI

Today the repository is primarily structured around:

- `appCataloga`: operational pipeline and workers
- `MariaDB`: `BPDATA` + `RFDATA`
- `webfusion`: browser-based query and task interface
- infrastructure and integration modules such as OpenVPN, nginx, Zabbix, and
  Grafana

## System Context

RF.Fusion was designed for a distributed spectrum-monitoring environment with
remote monitoring stations, central services, shared storage, and user-facing
publication and analytics layers.

![General Diagram for the Spectrum Monitoring Network](./docs/images/general_diagram.svg)

## Project Architecture

At a practical level, the current system behaves like this:

1. monitoring stations generate raw measurement files
2. `appCataloga` discovers, backs up, and processes those files
3. operational state is stored in `BPDATA`
4. spectra and repository catalog data are stored in `RFDATA`
5. payloads live in the shared repository mount (`reposfi`)
6. `webfusion` queries both databases and serves the user-facing workflow

Main runtime responsibilities:

- `appCataloga`
  - remote host discovery
  - SFTP backup
  - local and external processing
  - task queue orchestration

- `MariaDB`
  - `BPDATA`: hosts, host tasks, file tasks, history
  - `RFDATA`: sites, equipment, spectra, repository file catalog

- `webfusion`
  - station map centered on Brazil
  - spectrum query in `Spectrum` and `File` modes
  - host inspection
  - host-task creation
  - direct repository-backed downloads through `nginx`

The current RF.Fusion platform-level organization is represented below.

![General Workflow for the Spectrum Monitoring Network](./docs/images/HLD-RFFusion.svg)

## Main Components

### appCataloga

`appCataloga` is the operational core of the project.

It contains the worker scripts that:

- discover files on remote hosts
- back them up to the server repository
- process spectra locally or through `appAnalise`
- update `BPDATA` and `RFDATA`

Its operational workflow is summarized below.

![RF.Fusion Operational Workflow](./docs/images/RFFusion-workflow.svg)

Main documentation:

- [appCataloga overview](./src/appCataloga/README.md)

### webfusion

`webfusion` is the current browser interface for the project.

It provides:

- station map with quick actions
- spectrum and file query screens
- host query
- task creation

Main documentation:

- [webfusion module README](./src/webfusion/README.MD)
- [web container README](./install/webserver/README.MD)

### Database Layer

The repository uses two project databases:

- `BPDATA`
- `RFDATA`

Main documentation:

- [MariaDB container README](./install/mariaDB/README.md)
- [database interconnections](./src/mariadb/scripts/DB_INTERCONNECTIONS.md)

### Infrastructure Modules

Additional repository areas cover network and service integration, including:

- OpenVPN
- nginx
- Zabbix
- Grafana

Relevant module docs:

- [OpenVPN](./src/ovpn/README.md)
- [nginx](./src/nginx/README.md)
- [Zabbix](./src/zabbix/README.md)

## Repository Layout

```text
RF.Fusion/
├── cemetery/        # archived legacy material
├── docs/            # project images and reference docs
├── install/         # container and deployment material
├── src/             # application and service source code
├── test/            # active validation suite
└── data/            # project data area
```

Important subtrees:

- `install/appCataloga`
- `install/mariaDB`
- `install/webserver`
- `src/appCataloga`
- `src/webfusion`
- `src/mariadb/scripts`
- `test/`

## Deployment Entry Points

The most relevant container deployment documentation today is:

- [appCataloga container](./install/appCataloga/README.md)
- [MariaDB container](./install/mariaDB/README.md)
- [webfusion/webserver container](./install/webserver/README.MD)

These documents reflect the current Linux/Podman-oriented deployment flow.

## Testing

The active test suite now lives under `test/`.

The legacy test material was intentionally archived under `cemetery/` and
should not be treated as the current validation baseline.

Current test entry points include:

- `test/tests/shared`
- `test/tests/stations`
- `test/tests/workers`
- `test/tests/db`

Typical execution:

```bash
cd /RFFusion/test
pytest tests -q
```

Additional test documentation:

- [test suite README](./test/README.md)

## Current State

The repository has a mix of mature operational code and inherited historical
structure.

In practical terms:

- the `appCataloga` pipeline is functional and significantly more robust than
  its earlier state
- `webfusion` has evolved from a thin query page into a useful operational UI
- deployment documentation for the active containers has been refreshed
- the project still carries legacy areas and some schema-level heuristics,
  especially around cross-database correlation such as `site -> host`

## Contributing

Contributions are welcome, especially when they improve:

- operational robustness
- documentation quality
- test coverage
- schema clarity
- maintainability of the worker pipeline

When working on the repository, prefer the currently active documentation and
test suite over legacy material archived under `cemetery/`.

## License

Distributed under the GNU General Public License (GPL), version 3.

See:

- [LICENSE.txt](./LICENSE)

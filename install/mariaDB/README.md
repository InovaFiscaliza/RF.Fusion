# MariaDB Container

This directory contains the Linux deployment material for the RF.Fusion
MariaDB container.

Its purpose is to provide the relational backend used by the project,
especially:

- `BPDATA`, used by `appCataloga`
- `RFDATA`, used by `webfusion`, spectrum cataloging, and measurement storage
- `RFFUSION_SUMMARY`, used by `webfusion` and external consumers for
  materialized summary reads

## Directory Layout

- [/RFFusion/install/mariaDB/linux/Containerfile](/RFFusion/install/mariaDB/linux/Containerfile)
- [/RFFusion/install/mariaDB/linux/docker-entrypoint.sh](/RFFusion/install/mariaDB/linux/docker-entrypoint.sh)
- [/RFFusion/install/mariaDB/linux/deploy-debian12-mariadb.sh](/RFFusion/install/mariaDB/linux/deploy-debian12-mariadb.sh)

## Internal Architecture

The container runs:

1. Debian 12
2. MariaDB Server
3. MariaDB Client
4. `sshd`

Startup flow:

1. the entrypoint prepares SSH
2. it initializes `/var/lib/mysql` if needed
3. it starts a temporary MariaDB instance on socket mode
4. it creates the initial users and `appdb`
5. it shuts down the temporary server
6. it starts MariaDB bound to `0.0.0.0`
7. it keeps SSH in the foreground

After the container is up, the deployment script applies the project schemas:

- [/RFFusion/src/mariadb/scripts/createProcessingDB-v9.sql](/RFFusion/src/mariadb/scripts/createProcessingDB-v9.sql)
- [/RFFusion/src/mariadb/scripts/createMeasureDB-v5.sql](/RFFusion/src/mariadb/scripts/createMeasureDB-v5.sql)
- [/RFFusion/src/mariadb/scripts/createFusionSummaryDB-v1.sql](/RFFusion/src/mariadb/scripts/createFusionSummaryDB-v1.sql)
- [/RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v2-error-aggregation.sql](/RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v2-error-aggregation.sql)
- [/RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v3-refresh-events.sql](/RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v3-refresh-events.sql)

## Host Prerequisites

The current Linux deployment assumes:

- project repository at `/RFFusion-dev/RF.Fusion`
- `podman` installed and working
- network `podman` already available

## How To Deploy

Enter the Linux deployment directory:

```bash
cd /RFFusion/install/mariaDB/linux
```

Grant execution permission to the scripts:

```bash
chmod +x *
```

Run the deployment:

```bash
./deploy-debian12-mariadb.sh
```

## What The Deployment Does

The deployment script:

1. switches the Podman context
2. rebuilds the image `debian12-mariadb`
3. removes the previous container if present
4. creates a new container with:
   - static IP `10.88.0.33`
   - SSH host port `2224`
   - MariaDB host port `9081`
5. checks whether the container reached `running`
6. executes the RF.Fusion SQL initialization scripts inside the container

## Published Services

After deployment, the expected access points are:

- MariaDB: `127.0.0.1:9081`
- SSH: `ssh root@127.0.0.1 -p 2224`

## Database Initialization

The entrypoint itself guarantees the MariaDB runtime is bootstrapped.

The deployment script then loads the RF.Fusion schemas:

- `BPDATA` from `createProcessingDB-v9.sql`
- `RFDATA` from `createMeasureDB-v5.sql`
- `RFFUSION_SUMMARY` from `createFusionSummaryDB-v1.sql`
- `RFFUSION_SUMMARY` refinements from the `v2` and `v3` migration scripts

The MariaDB container is also configured with `event_scheduler=ON`, which is
required for the periodic `RFFUSION_SUMMARY` refresh event.

This means the deploy script is responsible for turning the generic MariaDB
container into the RF.Fusion project database.

## Important Operational Note

The current deployment script mounts:

- `/RFFusion-dev/RF.Fusion -> /RFFusion`

but it does not mount an external persistent volume for `/var/lib/mysql`.

So, with the current script, recreating the container is effectively a fresh
database bootstrap operation. That is acceptable for rebuild-oriented setups,
but it is not the same as a durable external database volume strategy.

## Network Note

Legacy network helper scripts are no longer part of the supported deployment
flow for this container.

The current deployment script is the authoritative source and expects the
`podman` network to already exist.

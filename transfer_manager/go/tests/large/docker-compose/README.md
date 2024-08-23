# `docker` tests of Transfer

This directory contains `docker`-based tests of Transfer.

## Run

In order to run these tests, the environment must be properly configured. In addition to a working `ya make`, the following are required:

1. `docker` installed. [Guide](https://docs.docker.com/engine/install/)
2. `docker-compose` installed. [Guide](https://docs.docker.com/compose/install/)
3. Docker must be logged into `registry.yandex.net`. [Guide](https://docs.yandex-team.ru/sdc/development/docker/installation#docker-registry)
4. Execute `sudo sysctl -w vm.max_map_count=262144` to set a system parameter to a proper value to run Elasticsearch
5. Also should be given IDM role (Префиксы / data-transfer/ / contributor) for system (Docker-registry) - like that: https://idm.yandex-team.ru/roles/174720484?section=history (@ovandriyanov gave it to data-transfer team). How to check if permissions enough: "docker pull registry.yandex.net/data-transfer/tests/postgres-postgis-wal2json:13-3.3-2.5@sha256:5ab2b7b9f2392f0fa0e70726f94e0b44ce5cc370bfac56ac4b590f163a38e110"

After this, use `ya make -ttt .` to conduct tests or `ya make -AZ` to canonize.

## Examples

This folder compose a list of reproducable examples of `trcli` binary.

Each folder contains a small `docker-compose.yaml` file with env-setup, `transfer.yaml` with config and `demo.tape`.

To record `demo.tape` we use [`vhs` cli](https://vhs.charm.sh/)

```shell
vhs demo.tape &&  vhs publish demo.gif
```

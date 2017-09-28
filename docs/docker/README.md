## OepnSetDocker Hub

The latest release can be found on **[Docker Hub](https://hub.docker.com/r/opset/openset_x64_rel/)**. 

##### To run from Docker Hub -

First, install or update Docker (get it **[here](https://www.docker.com/get-docker)**) on  your host machine or VM.

From the console type:

```bash
docker run -p 2020:2020 -e OS_HOST=<HOST IP> -e OS_PORT=2020 --rm=true -d opset/openset_x64_rel
```

> :pushpin: provide the IP or host name for the Host VM or machine with `-e OS_HOST=`. If you are setting up a one node instance, you remove `-e OS_HOST`.

##### To see your container instance (and get the ID) -
```bash
docker ps
```

##### To see what's happening inside (tailing forever) -
```bash
docker logs {{funny_name or container_id}} -f
```

##### To stop it -
```bash
docker stop {{funny_name or container_id}}
```
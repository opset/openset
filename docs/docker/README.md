## OpenSet on Docker Hub

The latest release can be found on **[Docker Hub](https://hub.docker.com/r/opset/openset_x64_rel/)**. 

##### To run from Docker Hub -

First, install or update Docker (get it **[here](https://www.docker.com/get-docker)**) on  your host machine or VM.

**Run package as a deamon:**

```bash
docker run -p 8080:8080 -e OS_HOST=127.0.0.1 -e OS_PORT=8080 --rm=true -d opset/openset_x64_rel
```
**Run package locally:**
```bash
docker run -p 8080:8080 -e OS_HOST=127.0.0.1 -e OS_PORT=8080 --rm=true -it opset/openset_x64_rel
```
- `OS_PORT` is the port on the host where OpenSet will answer.
- `OS_HOST` is the host IP or hostname.
- `PORT:PORT` maps the host port to a container port, to answer on port `80` you would map `80:8080`.

> :pushpin: provide the IP or host name for the host os with `-e OS_HOST=`. If you are setting up a one node instance, you skip `-e OS_HOST`.

##### To see your container instance (and get the ID) -
```bash
docker ps
```

##### To see what's happening inside  -
```bash
docker logs {{funny_name or container_id}} -f
```
> :pushpin: use `-f` to tail log forever.

##### To stop it -
```bash
docker stop {{funny_name or container_id}}
```
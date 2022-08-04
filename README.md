# Degoo Drive

Because Degoo does not provide a way to use their application as a virtual drive, like Google Drive, I have created a project to make this possible, thanks to [pyfuse3](https://github.com/libfuse/pyfuse3) and [Degoo CLI](https://github.com/bernd-wechner/Degoo). This **will not work on Windows**, only available in unix environments

## Requirements

* pyfuse3 requires the **libfuse3-dev** and **fuse3** libraries to run. These can be installed via ``apt install -y libfuse3-dev fuse3`` (Available from **Ubuntu 20.04**)
* Degoo CLI requires **python 3.8** and dependencies included in [requirements.txt](https://github.com/bernd-wechner/Degoo/blob/master/requirements.txt)
* And this project requires both of the above

### Dependencies

````shell
apt update
apt install -y --no-install-recommends git
apt install -y libfuse3-dev fuse3
apt install -y python3.8
apt install -y python3-pip
apt install -y pkg-config
apt install -y python3.8-dev

python3.8 -m pip install appdirs wget python-magic humanize humanfriendly python-dateutil requests clint requests_toolbelt pyfuse3 pyjwt
````

All in one:
````shell
apt update && apt install -y --no-install-recommends git libfuse3-dev fuse3 python3.8 python3-pip pkg-config && python3.8 -m pip install appdirs wget python-magic humanize humanfriendly python-dateutil requests clint requests_toolbelt pyfuse3
````

## Installation

You must clone this repository ``git clone https://github.com/MDKPredator/degoo_drive``. Ensure you have installed git: ``apt install git``

## Configuration

Run **degoo_login** to create the directory with the configuration to access Degoo. Normally it will be created in ```~/.config/degoo```. In this path you should have the following files:
* default_properties.txt
* schedule.json
* credentials.json
* keys.json

Edit file **credentials.json** to enter your credentials (email and password)

**IMPORTANT:** If you log in with Google (**Sign in with Google** button) this method does not work. You will have to change it to login with your email and a password.

## Basic usage

First you must clone this project with ``git clone https://github.com/MDKPredator/degoo_drive``. You can then mount your unit by running the command `python3 fuse_degoo.py`. By default, drive is mounted in `/home/degoo`

## Options

The following options are available:

* ``--mountpoint`` Path where drive will be mounted. Default is `/home/degoo`
* ``--degoo-email`` Email to login in Degoo
* ``--degoo-pass`` Password to login in Degoo
* ``--degoo-token`` Token for requests. Alternative if login fails
* ``--degoo-refresh-token`` Used when token expires. Alternative if login fails
* ``--degoo-path`` Degoo base path to mount the drive. Default is root `/`
* ``--cache-size`` Size of downloaded chunk
* ``--debug`` Displays logs
* ``--debug-fuse`` Displays the filesystem logs
* ``--allow-other`` Allows other users to access files
* ``--refresh-interval`` Time, in minutes, that Degoo information is refreshed. Default is 10 minutes
* ``--disable-refresh`` Disables the refresh
* ``--flood-sleep-time`` Waiting time, in seconds, before resuming requests once the maximum has been reached
* ``--flood-max-requests`` Maximum number of requests in the period
* ``--flood-time-to-check`` Request control period, in minutes
* ``--change-hostname`` Disable change domain for media files. Default is **false**
* ``--mode`` How to read the content. **Lazy** (default) reads as browsing (recommended for large content). **Eager** to read all content the first time
* ``--config-path`` Path to the configuration files. Default is **~/.config/degoo/**
* ``--plex-split-file`` (Experimental) Check if there are split files to cache the first part

## Docker

This project includes a **Dockerfile** to mount the virtual drive. You will only need to modify the **credentials.json** file before creating the image:

1. Clone this repository: ``git clone https://github.com/MDKPredator/degoo_drive``
2. Modify ``degoo_config/credentials.json``
3. Create the image ``docker build -t degoo_drive .``
4. Run container ``docker run -dit --privileged --name degoo degoo_drive``

### Examples

Here are some examples of how to pass arguments to the mounting drive

* Run container with debug: ``docker run -dit --privileged --name degoo degoo_drive --debug``
* Run container bypassing login (token and refrehs token): ``docker run -dit --privileged --name degoo degoo_drive --debug --degoo-token myLoginToken --degoo-refresh-token myRefreshToken``
* Change default mountpoint: ``docker run -dit --privileged --name degoo degoo_drive --debug --mountpoint /degoo_drive``

## Degoo Drive and Plex

You can also mount the virtual drive to use plex in a docker container. You will need the latest versions of plex which you can find [here](https://hub.docker.com/r/linuxserver/plex/tags). They should be at least the __bionic__ versions that include the **libfuse3-dev** and **fuse3** libraries, or higher. You can follow the steps below to do so: 

1. First creates plex container:
   
    ````shell
    docker create --name=plex --net=host --memory="2gb" --privileged --cap-add SYS_ADMIN --device /dev/fuse -e VERSION=latest -e PUID=1001 -e PGID=1001 -e TZ=Europe/Madrid -v /home/plex/config:/config -v /home/plex/tvshows:/data/tvshows -v /home/plex/movies:/data/movies -v /home/plex/transcode:/transcode linuxserver/plex:version-1.21.3.4021-5a0a3e4b2
    ````

2. Start container: ``docker start plex`` 
3. Enter the container: ``docker exec -it plex bash``
4. Install dependencies ([Requirements](#requirements) section)
5. Clone this repository
6. Enter the directory: ``cd degoo_drive``
7. Log in Degoo: ``python3 degoo_login``. You will see the message:
````shell
No login credentials available. Please add account details to /root/.config/degoo/credentials.json
No properties are configured or available. If you can find the supplied file 'default_properties.txt' copy it to '/root/.config/degoo/default_properties.txt' and try again.
Login failed.
````
8. Enter in **degoo_config** directory and modify the **credentials.json** file to enter your email and password
9. Copy **credentials.json** and **default_properties.txt** to degoo config directory: ``cp credentials.json default_properties.txt /root/.config/degoo/``
10. Re-run **degoo_login** to make sure all is correct. Remember to return to the previous directory: ``cd .. && python3 degoo_login``. You should now see the message ``Successfuly logged in.``
11. Finally, mount the unit: ``mkdir -p /home/degoo && python3 fuse_degoo.py --debug --allow-other &``

# Login bypass

If the login mechanism fails (lately cloudflare seems to cut the call with a **429 Too Many Requests**), you can bypass it by using directly the **token** and **refreshToken** provided by Degoo once logged in. To do so, go to https://app.degoo.com/login and open the developer tools to see this data.

![Degoo login](images/degoo_token_refresh_token.png)

# Disclaimer

Uploading files to Degoo (**cp** and **mv** commands) does not work 100% of the time, so it is recommended to use the **cp** command to avoid losing files. In case the upload is unsuccessful, if you have enabled the logs, you will see the following message: **WARN: file [name_of_file.ext] has not been uploaded successfully**

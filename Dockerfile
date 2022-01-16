FROM python:3.8

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get install -y pkg-config \
    && apt-get install -y libfuse3-dev fuse3

RUN python -m pip install appdirs wget python-magic humanize humanfriendly python-dateutil requests clint requests_toolbelt pyfuse3 PyJWT

RUN mkdir -p degoo_drive/degoo \
    && mkdir -p /root/.config/degoo/ \
    && mkdir -p /home/degoo

COPY fuse_degoo.py degoo_drive
COPY setup.py degoo_drive
COPY degoo/__init__.py degoo_drive/degoo
COPY src degoo_drive/src

COPY degoo_config/credentials.json /root/.config/degoo/
COPY degoo_config/default_properties.txt /root/.config/degoo/
COPY degoo_config/keys.json /root/.config/degoo/
COPY degoo_config/schedule.json /root/.config/degoo/

ENTRYPOINT ["python3.8", "/degoo_drive/fuse_degoo.py"]

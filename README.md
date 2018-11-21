# sdfs-client

## Get-started

1. Create Python's virtual environment in *venv/* and install requirements:
```
make
source venv/bin/activate
```
This command, also create *tmp/* directory with some files for testing.

2. You can manually run *./sdfs-client.py*:
```
usage: sdfs-client.py [-h] [--local LOCAL] [--logfile LOGFILE]
                      [--loglevel LOGLEVEL] [--nameserver NAMESERVER]
                      [--username USERNAME] [--password PASSWORD]
```

3. Or look into *./sdfs.sh* and adjust the parameters (if it needs),
then use it as entry point. By default *./sdfs.sh* will start in *tmp/*
and logging into *tmp/debug.log*.


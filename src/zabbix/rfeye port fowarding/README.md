# About port forwarding in RFeye

Since Debian Wheezy does not use systemd, you can utilize the traditional init system, which relies on SysVinit scripts to manage services. Here's how you can create a SysVinit script to run socat as a service:

## 1. Create the Init Script:

Copy the provided script to the /etc/init.d folder

```bash
wget https://raw.githubusercontent.com/src/zabbix/rfeye port fowarding/socat-forward -O /etc/init.d/socat-forward
```

This script is hardcoded to forward the port 9081 of the rfeye node to port 80 of the router or modem connected in address 192.168.10.254. You may want to change the script to fit your needs.

## 2. Set the permissions for the script:

```bash
chmod +x /etc/init.d/socat-forward
```

## 3. Add the service to the system's startup:

```bash
update-rc.d socat-forward defaults
```

## 4. Start the service:

```bash
service socat-forward start
```

## 5. Check the status of the service:

```bash
service socat-forward status
```

## 6. Final thoughts:

You may want to stop the service at some point. To do so, run the following command:

```bash
service socat-forward stop
```

Port forwarding is a useful tool for accessing services on a remote server but it may also pose a security risk. Always make sure to secure your server and services before enabling port forwarding.

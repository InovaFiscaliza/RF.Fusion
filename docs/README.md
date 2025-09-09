
# SystemCTL Cheats

| Server Action            | Command                                  |
| ------------------------ | ---------------------------------------- |
| List services            | `systemctl list-units --type=service`    |
| Start openvpn            | `systemctl start openvpn-server@server`  |
| Enable autostart openvpn | `systemctl enable openvpn-server@server` |
| Status openvpn            | `systemctl status openvpn-server@server` |
| Stop openvpn             | `systemctl stop openvpn-server@server`   |
| reboot server            | `systemctl reboot` |

| Client Action            | Command                                  |
| ------------------------ | ---------------------------------------- |
| Start openvpn client interatively for debug | `openvpn client.conf`  |
| Start openvpn client     | `systemctl start openvpn-client@client`  |
| Stop openvpn client     | `systemctl stop openvpn-client@client`  |
| Check openvpn client     | `systemctl status openvpn-client@client`  |
| Enable autostart openvpn | `systemctl enable openvpn-client@client`  |

| Client Action            | Command                                  |
| ------------------------ | ---------------------------------------- |
| Reload daemon            | `systemctl daemon-reload`  |
| Clean log            | `systemctl reset-failed`  |

# General Linux Cheats

| Action                   | Command                                  |
| ------------------------ | ---------------------------------------- |
| Compress tgz            | `tar -czvf name-of-archive.tar.gz /path/to/directory-or-file`    |
| Extract tgz            | `tar -xzvf archive.tar.gz`  |
| Generate IP list from CCD files | `for fn in *; do printf "$fn "; cat "$fn"; done` |
| List files to csv | `find -P -type f -printf "%h,%f,%CY-%Cm-%Cd %CT,%s,%u,%M\n" > files.csv` |

# Network Cheats

| Action                   | Command                                  |
| ------------------------ | ---------------------------------------- |
| List routing table       | `ip route`  |
| List ports and systens   | `lsof -i -P -n`  |
| List ports em process with corresponding states   | `ss -tulp` |
| Open UDP port 1194 for listening  | `nc -vvvlu 1194`  |
| Open UDP port 1194 as output | `nc -uvv rhfisnspdex01 1194` |

# Screen Cheats

| Action                   | Command                                  |
| ------------------------ | ---------------------------------------- |
| Starting Named Session | `screen -S session_name`
| Detach from Linux Screen Session | `Ctrl+a d`
| Reattach to a Linux Screen | `screen -r`
| List linux screen | `screen -ls`
| Reattach an specific screen | `screen -r 10835`

# SFTP Cheats

| Action                   | Command                                  |
| ------------------------ | ---------------------------------------- |
| Open connection with user lobao.lx do the dex01 server| `sftp lobao.lx@rhsnspdex01` |
| List remote files | `ls` |
| Download a file from the remote host | `get filename` |
| Download a recursively from the remote host | `get -r foldername` |
| Upload a file to the remote host | `put filename` |
| Close connection | `exit`|

# TCP DUMP Cheats

| Action                   | Command                                  |
| ------------------------ | ---------------------------------------- |
| Verbose capture of any interface | `tcpdump -eni any -vv \| grep 172.24` |
| Capture ICMP packages from tun0 interface | `tcpdump -eni tun0 icmp` |

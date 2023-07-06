<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a name="indexerd-md-top"></a>

<!-- PROJECT SHIELDS -->
<!--
*** based on https://github.com/othneildrew/Best-README-Template
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-RF.Fuse">About RF.Fuse</a></li>
    <li><a href="#background">Background</a></li>
      <ul>
        <li><a href="#monitoring-unit">Monitoring Unit</a></li>
        <li><a href="#network-server-core">Network Server Core</a></li>
      </ul>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

# About

This document provide a walkthrough the OpenVPN server instalation with some customization by the provided scripts.

Description originally based on <https://tecadmin.net/install-openvpn-centos-8/> but many editions were made to accommodate specific requirements for the Anatel Spectrum Monitoring Network

Additional useful references.
* <https://blog.securityevaluators.com/hardening-openvpn-in-2020-1672c3c4135a>
* <https://forums.openvpn.net/viewtopic.php?t=26839>
* <https://openvpn.net/community-resources/expanding-the-scope-of-the-vpn-to-include-additional-machines-on-either-the-client-or-server-subnet/>
* <https://openvpn.net/vpn-server-resources/troubleshooting-reaching-systems-over-the-vpn-tunnel/>

The following sections will guide you through a series of checks and procedures to implement the OpenVPN Server.

## Work Environment

The server is setup in a VM under VMWare and SenhaSegura managed infrasrtucture using the following versions

* Red Hat Enterprise Linux release 8.5 (Ootpa)
* OpenVPN 2.4.12 x86_64-redhat-linux-gnu [SSL (OpenSSL)] [LZO] [LZ4] [EPOLL] [PKCS11] [MH/PKTINFO] [AEAD] built on Mar 17 2022
* OpenSSL 1.1.1k  FIPS 25 Mar 2021, LZO 2.08
* EASY-RSA version 3.1

Clients are a mixed bag, including Mikrotik devices, debian 7.2 monitoring stations with OpenVPN 2.2.1 arm-linux-gnueabi built on Jun 19 2013 and stations running Windows 10 with up-to-date client applications.

## Disable SELinux

Check if SELinux is enabled and, if so, disable it. 

Security-Enhanced Linux (SELinux) is a security architecture developed by the United States National Security Agency (NSA) and defines access controls for the applications, processes, and files on a system.

It uses security policies to enforce what can or can’t be accessed by any process in the system and will block the openvpn server if not properly setup.

Server is supposed to be in a controlled environment, behind firewall and PAM security solutions, such as internal VM security may be lowered without relevant increase in risk

In short, it's hard to setup. Simply disable it.

Open the /etc/selinux/config file:

```bash
nano /etc/selinux/config
```

Change the following line:

```bash
SELINUX=disabled
```

Save the file (^O) and exit the editor (^X)

You may reboot in order to apply the changes or run:

```bash
setenforce 0
```

## Enable IP forwarding

IP forwarding allows the operating system to accept the incoming network packets and forward then, including to other network interfaces.

This allows direct routing between the intranet and the monitoring stations. To do so, 

Edit the file /etc/sysctl.conf:

```bash
nano /etc/sysctl.conf
```

Add the following line, anywhere in the file:

```bash
net.ipv4.ip_forward = 1
```

Run the following command to apply the changes:

```bash
sysctl -p
```

## Set system-wide cryptographic policies

The default cryptographic policy forRHEL 8 will refuse connections witH TLS versions below 1.2, such as used by OpenVPN clients with version below 2.3.3

You may check the current policy in the server using:

```bash
update-crypto-policies --show
```

Which may give you the standard answer as:

```bash
DEFAULT
```

To be enable the connection of older client you need to set it to `LEGACY` using the following command:

```bash
update-crypto-policies --set LEGACY
```

## Install OpenVPN Server

By default, you will need to install the EPEL repository in your system in order to install the latest version of OpenVPN.

Run the following command to install the ELEP repository:

```bash
yum install epel-release -y
```

Once installed, run the following command to install the latest version of OpenVPN:

```bash
yum install openvpn -y
```

Once the installation has been completed, you will also need to download easy-rsa for managing SSL certificates.

Find latest easy-rsa release from the repository <https://github.com/OpenVPN/easy-rsa/releases> and download it to a working folder

```bash
cd /etc/openvpn
wget https://github.com/OpenVPN/easy-rsa/releases/download/v3.1.0/EasyRSA-3.1.0.tgz
```

Next, run the following command to extract the downloaded file:

```bash
tar -xvzf EasyRSA-3.1.0.tgz
```

Next, rename the extracted directory to the easy-rsa:

```bash
mv EasyRSA-3.1.0 easy-rsa
```

## Initialize the Key Infrastructure (PKI)

Easy RSA uses a set of scripts to generate keys and certificates.

First, you will need to configure the Certificate Authority on your system.

To do so, change the directory to /etc/openvpn/easy-rsa and initialize the pki folder

```bash
cd easy-rsa
./easyrsa init-pki
```

If it is a fresh start you will get the following output:

```bash
* Notice:

  init-pki complete; you may now create a CA or requests.

  Your newly created PKI dir is:
  * /etc/openvpn/easy-rsa/pki

* Notice:
  IMPORTANT: Easy-RSA 'vars' file has now been moved to your PKI above.
```

Otherwise, the output will be as follows:

```bash
WARNING!!!

You are about to remove the EASYRSA_PKI at:
* /etc/openvpn/easy-rsa/pki

and initialize a fresh PKI here.

Type the word 'yes' to continue, or any other input to abort.
  Confirm removal: yes

* Notice:

  init-pki complete; you may now create a CA or requests.

  Your newly created PKI dir is:
  * /etc/openvpn/easy-rsa/pki

* Notice:
  IMPORTANT: Easy-RSA 'vars' file has now been moved to your PKI above.
```

There may be created a /pki/vars file from the standard vars.example file.

This example file has a template with detailed explanation of all options. It is useful to better understand this configuration

## Using provided extra scripts aid

Several scripts were created to make it easier to deploy the server.

Upload the following scripts to the server using your preferred SFTP service

| Script/File                                       | Use |
| ------------------------------------------------- | -- |
| `/etc/openvpn/easy-rsa/deploy`                    | Script that deploys all other scripts and configuration files listed below. Should be run after openvpn installation  |
| `/etc/openvpn/easy-rsa/pki/vars`                  | pre-configured vars file as discussed below |
| `/etc/openvpn/easy-rsa/kkreate`                   | Create a single client package with multiple files in tgz format |
| `/etc/openvpn/easy-rsa/ukkreate`                  | Create a single client package with a single file in unified .ovpn format |
| `/etc/openvpn/easy-rsa/batch_kk`                  | Create clients in batch based in the list configured in the specified conf file as per example rfeye.conf |
| `/etc/openvpn/easy-rsa/batch_kk`                  | Create clients in batch using the tgz package format based in the list configured in the specified conf file as per example rfeye.conf |
| `/etc/openvpn/easy-rsa/batch_ukk`                 | Create clients in batch using the unified format based in the list configured in the specified conf file as per example ums.conf |
| `/etc/openvpn/easy-rsa/build_ccd`                 | Build de client specific configuration files with assigned fixed IP to each client |
| `/etc/openvpn/easy-rsa/cwsm.conf` | Configuration file used by build_ccd and batch_kk to create ccd and keys in batch for CWSM RMU stations |
| `/etc/openvpn/easy-rsa/ermx.conf` | Configuration file used by build_ccd and batch_kk to create ccd and keys in batch for ERMx stations |
| `/etc/openvpn/easy-rsa/miaer.conf` | Configuration file used by build_ccd and batch_kk to create ccd and keys in batch for MIAer stations |
| `/etc/openvpn/easy-rsa/rfeye.conf` | Configuration file used by build_ccd and batch_kk to create ccd and keys in batch for RFEye Node stations |
| `/etc/openvpn/easy-rsa/ums.conf` | Configuration file used by build_ccd and batch_kk to create ccd and keys in batch for UMS300 stations |
| `/etc/openvpn/easy-rsa/rme_server.conf` |Configuration file used by build_ccd and batch_kk to create ccd and keys in batch for RME Server network |
| `/etc/openvpn/server/server.conf`                 | Server configuration file |
| `/etc/openvpn/easy-rsa/client_script/client.conf` | Template for client configuration file. Tag `<client>` will be replaced by the client name from the used configuratio file when building the package by batch_kk or batch_ukk|
| `/etc/openvpn/easy-rsa/client_script/up.sh`       | Script to be run on linux clients when connection goes up |
| `/etc/openvpn/easy-rsa/client_script/down.sh`     | Script to be run on linux clients when connection goes down |

Once all files are copied into a folder, you may need to set the proper home path in the deploy script

Afterwards run de deploy script by the following commands in order to configure all scripts

```bash
chmod 700 deploy
./deploy
```

## `/etc/openvpn` Folder tree

The end file organization obtained is according to the following pattern (reordered to highlight the importat files and folders first):

```bash
.
├── openvpn-status.log              #> VPN status, update every time a client connects or disconnects
├── easy-rsa
│   ├── batch_kk                    #> Build multiple client distribution packages (multi-file tgz) 
│   ├── batch_ukk                   #> Build multiple client distribution files in unified format .ovpn
│   ├── build_ccd                   #> Build the client configuration files ( define fixed IP )
│   ├── deploy                      #> Script to copy and set properties to RME files
│   ├── kkreate                     #> Create a single client configuration file (multi-file tgz)
│   ├── ukkreate                    #> Create a single client configuration file in unified format .ovpn
│   ├── cwsm.conf                   #> Configuration file for CWSM RMU stations
│   ├── ermx.conf                   #> Configuration file for ERMx stations
│   ├── miaer.conf                  #> Configuration file for MIAer stations
│   ├── rfeye.conf                  #> Configuration file for RFEye Node stations
│   ├── ums.conf                    #> Configuration file for UMS300 stations
│   ├── rme_server.conf             #> Configuration file for RME Server network
│   ├── ovpn_config.md              #> A copy of this file
│   ├── script_template             #> Scripts templates to be loaded in client packages
│   │   ├── client.conf             #> Standard client configuration file
│   │   ├── down.sh                 #> Original client down script from rfeye node, by CRFS
│   │   ├── up.sh                   #> Original client up script from rfeye node, by CRFS
│   ├── pki
│   │   ├── ca.crt
│   │   ├── certs_by_serial
│   │   │   ├── *.pen               #> multiple CLIENT_CERTIFICATE_IN_PEM_FORMAT.pem
│   │   ├── dh.pem
│   │   ├── index.txt
│   │   ├── index.txt.attr
│   │   ├── index.txt.old
│   │   ├── issued
│   │   │   ├── server.crt
│   │   │   └── *.crt               #> multiple CLIENT_CERTIFICATE_FILES.crt
│   │   ├── openssl-easyrsa.cnf
│   │   ├── pkg
│   │   │   ├── *.tgz               #> multiple CLIENT_KEYS_AND_CONFIGURATION_PACKAGES.tgz
│   │   │   └── *.ovpn              #> multiple CLIENT_KEYS_AND_CONFIGURATION_IN_UNIFIED_FORMAT.ovpn
│   │   ├── private
│   │   │   ├── ca.key
│   │   │   ├── server.key
│   │   │   └── *.tgz               #> CLIENT_KEYS.tgz
│   │   ├── reqs
│   │   │   ├── *.req               #> multiple CLIENT_REQUISITIONS_USED_TO_GENERATE_KEYS.tgz
│   │   │   └── server.req
│   │   ├── revoked                 #> Revoked certificates should b moved in this folder
│   │   │   ├── certs_by_serial
│   │   │   ├── private_by_serial
│   │   │   └── reqs_by_serial
│   │   ├── safessl-easyrsa.cnf
│   │   ├── serial
│   │   ├── tmp
│   │   └── vars                    #> Server easy-rsa configuration variables
│   ├── ChangeLog
│   ├── COPYING.md
│   ├── easyrsa
│   ├── gpl-2.0.txt
│   ├── mktemp.txt
│   ├── openssl-easyrsa.cnf
│   ├── README.md
│   ├── README.quickstart.md
│   ├── doc
│   │   ├── EasyRSA-Advanced.md
│   │   ├── EasyRSA-Readme.md
│   │   ├── EasyRSA-Upgrade-Notes.md
│   │   ├── Hacking.md
│   │   └── Intro-To-PKI.md
│   └── x509-types
│       ├── ca
│       ├── client
│       ├── code-signing
│       ├── COMMON
│       ├── email
│       ├── kdc
│       ├── server
│       └── serverClient
├── server
│   ├── ca.crt
│   ├── dh.pem                      #> Server configuration files
│   ├── openvpn-status.log          #> Client status
│   ├── server.conf                 #> Server configuration files
│   ├── server.crt                  #> Server configuration certificate
│   ├── server.key                  #> Server configuration key
│   └── ccd                         #> Client specific configuration files
│       └── ipp.txt                 #> DHCP IP assignations
├── client                          #> Not used in the server
└── readme.txt
```

## Building the CA

You may edit the configuration file by the following steps.

```bash
nano /etc/openvpn/easy-rsa/pki/vars
```

The example provided, used for the RME is presented below:

```bash
# Easy-RSA 3 parameter settings

# NOTE: If you installed Easy-RSA from your package manager, do not edit
# this file in place -- instead, you should copy the entire easy-rsa directory
# to another location so future upgrades do not wipe out your changes.

# HOW TO USE THIS FILE
#
# vars.example contains built-in examples to Easy-RSA settings. You MUST name
# this file "vars" if you want it to be used as a configuration file. If you do
# not, it WILL NOT be automatically read when you call easyrsa commands.
#
# It is not necessary to use this config file unless you wish to change
# operational defaults. These defaults should be fine for many uses without the
# need to copy and edit the "vars" file.
#
# All of the editable settings are shown commented and start with the command
# "set_var" -- this means any set_var command that is uncommented has been
# modified by the user. If you are happy with a default, there is no need to
# define the value to its default.

# A little housekeeping: DO NOT EDIT THIS SECTION
#
# Easy-RSA 3.x does not source into the environment directly.
# Complain if a user tries to do this:

if [ -z "$EASYRSA_CALLER" ]; then
        echo "You appear to be sourcing an Easy-RSA *vars* file." >&2
        echo "This is no longer necessary and is disallowed. See the section called" >&2
        echo "*How to use this file* near the top comments for more details." >&2
        return 1
fi

set_var EASYRSA                 "$PWD"
set_var EASYRSA_PKI             "$EASYRSA/pki"
set_var EASYRSA_TEMP_DIR        "$EASYRSA_PKI"
set_var EASYRSA_DN              "org"
set_var EASYRSA_REQ_COUNTRY     "BR"
set_var EASYRSA_REQ_PROVINCE    "Brasilia"
set_var EASYRSA_REQ_CITY        "Brasilia"
set_var EASYRSA_REQ_ORG         "anatel gov br"
set_var EASYRSA_REQ_EMAIL       "8a298574.anatel.gov.br@amer.teams.ms"
set_var EASYRSA_REQ_OU          "FI"
set_var EASYRSA_KEY_SIZE        2048
set_var EASYRSA_ALGO            rsa
set_var EASYRSA_CURVE           secp384r1
set_var EASYRSA_CA_EXPIRE       7210
set_var EASYRSA_CERT_EXPIRE     7210
set_var EASYRSA_CRL_DAYS        7210
set_var EASYRSA_CERT_RENEW      30
set_var EASYRSA_RAND_SN         "yes"
set_var EASYRSA_NS_SUPPORT      "no"
set_var EASYRSA_NS_COMMENT      ""
set_var EASYRSA_TEMP_FILE       "$EASYRSA_PKI/extensions.temp"
set_var EASYRSA_EXT_DIR         "$EASYRSA/x509-types"
set_var EASYRSA_SSL_CONF        "$EASYRSA/openssl-easyrsa.cnf"
set_var EASYRSA_DIGEST          "SHA256"
set_var EASYRSA_BATCH           "Y"
```

If any edition was performed,  save the file (`^O`) and exit the editor (`^X`)

Some of the variables are validate by the easy-rsa and will return error if not properly set. A known issue is with the country info, that must be a two letter code.

Next, build the CA certificates with the following command:

```bash
cd /etc/openvpn/easy-rsa
./easyrsa build-ca nopass
```

You should get the following output:

```
* WARNING:

Unsupported  characters are present in the vars file.
These characters are not supported: (') (&) (`) ($) (#)
Sourcing the vars file and building certificates will probably fail ..

................................................................................................+++++
........................................+++++
```

The `nopass` option is required in order to avoid cryptograph the key file.

A non-cryptographic key file is required in order to allow later batch generation of client keys.

The warning is due to the comments and default bash commands within the vars file. It can be safely ignored.

At the end of this procedure two files will be created:

```bash
./easy-rsa/pki/ca.crt
./easy-rsa/pki/private/ca.key
```

## Generate Server Certificate Files

Generate a key-pair and certificate request for your server.

Run the following command to generate the server key:

```bash
./easyrsa gen-req server nopass
```

You should get the following output:

```bash
* WARNING:

Unsupported  characters are present in the vars file.
These characters are not supported: (') (&) (`) ($) (#)
Sourcing the vars file and building certificates will probably fail ..

Generating a RSA private key
......+++++
.....................................+++++
writing new private key to '/etc/openvpn/easy-rsa/pki/d303b85e/temp.6459fdc0'
-----
```

This will create the following files

```bash
./easy-rsa/pki/reqs/server.req
./easy-rsa/pki/private/server.key
```

## Sign the Server Key Using CA

Sign the SFIAnatel key using your CA certificate:

Run the following command to sign the server key:

```bash
./easyrsa sign-req server server
```

You should get the following output:

```bash
* WARNING:

Unsupported  characters are present in the vars file.
These characters are not supported: (') (&) (`) ($) (#)
Sourcing the vars file and building certificates will probably fail ..

Using configuration from /etc/openvpn/easy-rsa/pki/safessl-easyrsa.cnf.init-tmp
Check that the request matches the signature
Signature ok
The Subject's Distinguished Name is as follows
countryName           :PRINTABLE:'BR'
stateOrProvinceName   :ASN.1 12:'Brasilia'
localityName          :ASN.1 12:'Brasilia'
organizationName      :ASN.1 12:'anatel gov br'
organizationalUnitName:ASN.1 12:'FISF SFI'
commonName            :ASN.1 12:'server'
emailAddress          :IA5STRING:'8a298574.anatel.gov.br@amer.teams.ms'
Certificate is to be certified until Apr  9 17:55:54 2042 GMT (7210 days)

Write out database with 1 new entries
Data Base Updated

* Notice:
Certificate created at: /etc/openvpn/easy-rsa/pki/issued/server.crt
```

As stated, the `server.crt` file is created

Verify the generated certificate file with the following command:

```bash
openssl verify -CAfile pki/ca.crt pki/issued/server.crt 
```

If everything is fine, you should get the following output:

```bash
pki/issued/server.crt: OK
```

Next, run the following command to generate a strong Diffie-Hellman key to use for the key exchange:

```bash
./easyrsa gen-dh
```

You should get something like the following output:

```bash
* WARNING:

Unsupported  characters are present in the vars file.
These characters are not supported: (') (&) (`) ($) (#)
Sourcing the vars file and building certificates will probably fail ..

Generating DH parameters, 2048 bit long safe prime, generator 2
This is going to take a long time
...............................................................................................................+..................................................................................................................................+........................+.....................................................................................................................................+.................+..............................................................................................................................................................................................................................................................................................................+..........+...............................................................................................................................................................................................................+......................................................................+.........................................................................................................................................................................................+......+..............................................................................................................+......................+................................................................................................................................................................................+...............+.............................................................................................+......+......+.........................................................................................+................................................................................................................................................................+............................................................+.....................+.....................................+.................................................................................................................................................................+..............................................+...................................+.............................................................................................................................................................................................+.........................+.......................................................................................+.............................................+.................+..................................................................................+................................................................................................................................................................................................+...............................+.....................................................................................................................................................................................................+..........................................................+............................................................................+....................+...............................................................................................................+......................................................+............+...................................................................................................................................................................................................+...........................................................................................................................................................+.....................................................................................................+..................+..............+............+.....+.........................................................................................................................................................................................................................+...........................................................................................................................................+.............................................+............................................+......+..............................................+...........+.....+.....................+..+..........................+......................+.................................+.........................+.....+.................................................................................................................................................................................................................+................................+........................................................................................................................................................................................+............+........................................................................................................+..............................+................................+..............................................................................+....................................................................................................................................+............................+........................................................................................+.................................................................................................................................+.........+................................................................................................................................................................+...........+........................................................................................................+......................................................................................................................................................................................+........................................................................................................................................+........................................................................................................+..................................................................................................................................................................+....................................................................................................................................+............................+..................................................................................................+..............................................................................................................................................................................................................................................................................+................................................................................................................................+....................................+.......................+.................................++*++*++*++*
```

It should not too long for 2048 dh and will create the a file `./easy-rsa/pki/dh.pem`

After creating all certificate files, copy them to the /etc/openvpn/server/ directory:

```bash
cp ./pki/ca.crt /etc/openvpn/server/
cp ./pki/dh.pem /etc/openvpn/server/
cp ./pki/private/server.key /etc/openvpn/server/
cp ./pki/issued/server.crt /etc/openvpn/server/
```

## Configure OpenVPN Server

You may inspect and edit the

```bash
nano /etc/openvpn/server/server.conf
```

The suggested configuration is the following:

```bash
port 1194
proto UDP
dev tun
server 172.24.0.0 255.255.248.0
management 127.0.0.1 6001
topology subnet

ca /etc/openvpn/server/ca.crt
cert /etc/openvpn/server/server.crt
key /etc/openvpn/server/server.key
dh /etc/openvpn/server/dh.pem
client-config-dir /etc/openvpn/server/ccd
ifconfig-pool-persist /etc/openvpn/server/ccd/ipp.txt

log-append /var/log/openvpn.log
status /etc/openvpn/server/openvpn-status.log
verb 3

script-security 1
client-to-client
keepalive 10 120

auth SHA256
tls-server
cipher AES-256-CBC

push "explicit-exit-notify 3"
push "dhcp-option DOMAIN anatel.gov.br"
```

When finished, save the file (`^O`) and exit the editor (`^X`)

Detailed explanation for the above configuration as follows:

* `port 1194` as per default openvpn port
* `proto TCP` in order to be compatible with older Mikrotik devices.
* `dev tun` option is used in order to connect a single device mapped within the VPN with a single fixed IP. Simpler connection and less overhead.
* `server 172.24.0.0 255.255.248.0` applies the IP range `172.24.0.0/21 (mask 255.255.248.0)` was defined to avoid conflicts with the internal IP network and provide useful addresses from `172.24.0.3` up to `172.24.7.253` to be used as fixed IP by monitoring stations.
* `topology subnet` is used to allow better IP allocation. This will not be compatible with old windows clients (<=2.0.9) and linux clients without privileges to set the interface properties. None of these situations are expected within the RME.
* `management 127.0.0.1 6001` defines the localhost and port to be used to access the management interface via telnet
* `ca`, `cert`, `key`, `dh` define paths to keys and certificates used by the serve. All defined using full pathname to avoid problems.
* `ifconfig-pool-persist` defines the path to the ipp.txt that stores the allocated IP addresses in DHCP.
* `client-config-dir` defines the path to the client specific configuration (CCD). This is needed to associate the client CN to fixed IP within the VPN
* `log-append` defines the path to specific openvpn log file
* `status` defines a path to the status file, that will replicate the current connected clients list.
* `verb 3` defines the verbosity of the log, Default=1. Each level shows all info from the previous levels. Level 3 is recommended for a good summary of what's happening.
* `script-security 1` is left for the default value 1, that is safer and allow only for basic network commands. May need to decrease security in order to run scripts (2) or 3, to allow OpenVPN to call external commands and scripts.
* `client-o-client` used to allow clients to talk to each other. This is important during the test fase and may be disallowed in the future, forcing all traffic between clients to pass through the corporative firewall or be refused.
* `keepalive 10 120` directive causes ping-like messages to be sent back and forth over VPN so that each side knows when the other side has gone down. In his case, ping every 10 seconds, assume that remote peer is down if no ping received during a 120 second time period.
* `cipher AES-256-CBC` controls the cipher used for data over the VPN. The indicated alternative is the only alternative compatible with the current RHEL8 Repo OpenVPN release (2.4.12) and the oldest clients in the network (RFEye with OVPN 2.2.1). this must me forced to ensure wider cross-version compatibility. (<https://community.openvpn.net/openvpn/wiki/CipherNegotiation>)
* `tls-cipher selected ECDHE-RSA-AES256-GCM-SHA384` controls the cipher used in the control channel. The selected option is the most recommended and compatible with all clients. List of TLS available can be obtained using <openvpn --show-tls>.
* `auth SHA1` Default openvpn value used to reduce client overload and enable compatibility with Mikrotik routers.
* `push "route 172.24.0.0 255.255.248.0 172.24.0.1 2"` is needed to push the trafic in the OpenVPN network to the VPN server. Later other routes may be added to allow the VPN clients to connect to different machines within Anatel Network.
* `push "explicit-exit-notify 3"` In UDP client mode or point-to-point mode, send server/peer an exit notification if tunnel is restarted or OpenVPN process is exited. In client mode, on exit/restart, this option will tell the server to immediately close its client instance object rather than waiting for a timeout. The number 3 (default=1) controls the maximum number of attempts that the client will try to resend the exit notification message.
* `push "dhcp-option DOMAIN anatel.gov.br"`, push domain to clients

Following options were not used for the following reasons

* `persist-tun` not used in order to allow connections to drop along with the interfaces. Clients do connect as administrators and do not require the interface to be kept alive. Keeping alive the interface may cause multiple interface problems in RFEye Nodes
* `persist-key` options not used for similar reasons
* `push "redirect-gateway def1"` was removed to avoid using the openvpn to general traffic by the clients. This reduce the load in the VPN.
* `duplicate-cn` option is removed in order to make it easier to detect a key leak.
* `auth-nocache` option not used since there is no user password authentication.
* `push "dhcp-option DNS 172.24.0.1"` not used since no bind server was configured at this moment. Anatel server users the following DNS <cat /etc/resolv.conf> : 10.10.2.200; 10.10.1.81; 10.10.1.2. For linux clients, option is also not usefully and an up script is needed to perform the required action. There is immediate no need to push dns configurations to clients, since these will be servers and there will be no need to solve names in the direction client to server.
* `comp-lzo` Data compression was removed to avoid incompatibility with simpler clients.
* 'crl-verify /etc/openvpn/easy-rsa/pki/revoked/certs_by_serial'
* `tls-version-min` was not used since some old clients (earlier than 2.3.3) does not include more than one 1.0 TLS algotithm.
* `tls-cipher` not used to enable auto negotiation and be more flexible with the selection of the TLS cipher, allowing for multiple client versions.

# Start OpenVPN

Before starting you may

OpenVPN is now installed and configured. Start the OpenVPN service and enable it to automatically start after reboot:

```bash
systemctl start openvpn-server@server
```

You should get no message by starting the service but you can check the log file for errors:

```bash
cat /var/log/openvpn.log
```

Which should include the following in relation to the start-up process

```bash
WARNING: Using --management on a TCP port WITHOUT passwords is STRONGLY discouraged and considered insecure
OpenVPN 2.4.12 x86_64-redhat-linux-gnu [SSL (OpenSSL)] [LZO] [LZ4] [EPOLL] [PKCS11] [MH/PKTINFO] [AEAD] built on Mar 17 2022
library versions: OpenSSL 1.1.1k  FIPS 25 Mar 2021, LZO 2.08
MANAGEMENT: TCP Socket listening on [AF_INET]127.0.0.1:6001
Diffie-Hellman initialized with 2048 bit key
Deprecated TLS cipher name 'ECDHE-RSA-AES256-GCM-SHA384', please use IANA name 'TLS-ECDHE-RSA-WITH-AES-256-GCM-SHA384'
OpenSSL: error:0909006C:PEM routines:get_name:no start line
CRL: cannot read CRL from file /etc/openvpn/easy-rsa/pki/revoked/certs_by_serial
CRL: loaded 0 CRLs from file /etc/openvpn/easy-rsa/pki/revoked/certs_by_serial
ROUTE_GATEWAY 172.16.17.1/255.255.240.0 IFACE=ens192 HWADDR=00:50:56:b9:7b:a7
TUN/TAP device tun0 opened
TUN/TAP TX queue length set to 100
/sbin/ip link set dev tun0 up mtu 1500
/sbin/ip addr add dev tun0 local 172.24.0.1 peer 172.24.0.2
/sbin/ip route add 172.24.0.0/21 via 172.24.0.2
Could not determine IPv4/IPv6 protocol. Using AF_INET
Socket Buffers: R=[212992->212992] S=[212992->212992]
UDPv4 link local (bound): [AF_INET][undef]:1194
UDPv4 link remote: [AF_UNSPEC]
MULTI: multi_init called, r=256 v=256
IFCONFIG POOL: base=172.24.0.4 size=510, ipv6=0
IFCONFIG POOL LIST
Initialization Sequence Completed
```

You may also check if the process is running using:

```bash
systemctl status openvpn-server@server
```

Which should guive an output like:

```bash
● openvpn-server@server.service - OpenVPN service for server
   Loaded: loaded (/usr/lib/systemd/system/openvpn-server@.service; enabled; vendor preset: disabled)
   Active: active (running) since Thu 2022-07-14 14:43:14 -03; 9min ago
     Docs: man:openvpn(8)
           https://community.openvpn.net/openvpn/wiki/Openvpn24ManPage
           https://community.openvpn.net/openvpn/wiki/HOWTO
 Main PID: 2304556 (openvpn)
   Status: "Initialization Sequence Completed"
    Tasks: 1 (limit: 49305)
   Memory: 1.2M
   CGroup: /system.slice/system-openvpn\x2dserver.slice/openvpn-server@server.service
           └─2304556 /usr/sbin/openvpn --status /run/openvpn-server/status-server.log --status-version 2 --suppress-timestamps --cipher AES-256-GCM --ncp-ciphers AES-256-GCM:AES-128-GCM:AES-256-CBC:AES-128-CBC:BF-CBC --config server>

Jul 14 14:43:14 rhfisnspdex01.anatel.gov.br systemd[1]: Starting OpenVPN service for server...
Jul 14 14:43:14 rhfisnspdex01.anatel.gov.br systemd[1]: Started OpenVPN service for server.
```

To enable the automatic restart after reboot, use the following:

```bash
systemctl enable openvpn-server@server
```

Run the following command to verify the status of OpenVPN service:

```bash
systemctl list-unit-files | grep vpn
```

The output should indicate that it is indirectly enabled:

```bash
openvpn-client@.service                    disabled
openvpn-server@.service                    indirect
```

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- LICENSE -->
## License

Distributed under the GNU General Public License (GPL), version 3. See [`LICENSE.txt`](../../LICENSE) for more information.

For additional information, please check <https://www.gnu.org/licenses/quick-guide-gplv3.html>

This license model was selected with the idea of enabling collaboration of anyone interested in projects listed within this group.

It is in line with the Brazilian Public Software directives, as published at: <https://softwarepublico.gov.br/social/articles/0004/5936/Manual_do_Ofertante_Temporario_04.10.2016.pdf>

Further reading material can be found at:

* <http://copyfree.org/policy/copyleft>
* <https://opensource.stackexchange.com/questions/9805/can-i-license-my-project-with-an-open-source-license-but-disallow-commercial-use>
* <https://opensource.stackexchange.com/questions/21/whats-the-difference-between-permissive-and-copyleft-licenses/42#42>

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>


## Acknowledgments



<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[smn_overview]: https://github.com/FSLobao/RF.Fuse/tree/main/docs/images/general_diagram.svg

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

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

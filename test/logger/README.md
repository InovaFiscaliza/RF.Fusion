<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-logger-test">About Logger test</a></li>
    <li><a href="#logger-script-example">Logger Script Example</a></li>
    <li><a href="#json-output">JSON Output</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#references">References</a></li>
  </ol>
</details>

# About Logger test

CRFS RFEye Logger application is used to perform automated measurements for available CRFS equipment.

This section provides test configuration that shall be used with RFEye Logger to provide the UDP stream used by [queryLoggerUDP.py](../../src/zabbix/root/usr/lib/zabbix/externalscripts/queryLoggerUDP.py) source file.

This file was created with the intention to avoid comments within the example script itself

# Logger Script Example

[asd](#logger-script-example)
One must highlight the following section of the rfye logger script provided as test example to allow for the UDP integration with Zabbix Server: [Script2023_v5_Logger_Fixed.cfg](./Script2023_v5_Logger_Fixed.cfg)

```toml
## Streams block with as many streams as required
[streams]
a = file,"%(data_dir)s/%(strYear)s/%(unit)s_%(date)s_T%(time)s.bin"
c = http
d = udp, 8910
e = udp, 5555 ## extra 5555 port used for Zabbix integration ! this must be included !

[run timer 1]
...

## Timer block 2 is used solely for Zabbix integration
[run timer 2]
timer = 1 sec
mesg5 = 2, e, eval(log.streams.udpe.active), "<json>{"76_108MHz":{"scans":%(log.scans.scanPMRD_2.opns.peak1.runs)s,"trigger":%(log.scans.scanPMRD_2.opns.mesg.runs)s},"108_137MHz":{"scans":%(log.scans.scanPMEC_2.opns.peak1.runs)s,"trigger":%(log.scans.scanPMEC_2.opns.mesg.runs)s}}</json>"

...

[run scan PMRD_2]
scan = 1 sec, 0, 76, 108, 50, 0, dBuVm=1
peak0 = 110, ac, 5 mins, "PMRD 2023 (Faixa 2 de 4)."
peak1 = 110,  d, 1 scan, "PMRD 2023 (Faixa 2 de 4).", thresh=30

mask  =   2,  a, once, "Mascara 20MHz-6Ghz @ 40dBuVm", mask_FM.csv, 100000
mesg  =   3,  c, eval (log.scans.scanPMRD_2.trigger), "{"type": 5, "hostname": "%(vars.unit)s", "message": "Rompimento de mascara em %(scan.start)s-%(scan.stop)s."}"

...

[run scan PMEC_2]
scan  = 1 sec, 0, 108, 137, 25, 0
peak0 = 310, ac, 1 min , "PMEC 2023 (Faixa 2 de 10)."
peak1 = 310,  d, 1 scan, "PMEC 2023 (Faixa 2 de 10).", thresh=-90

mask  =   2,  a, once, "Mascara 20MHz-6Ghz @ -80dBm", mask_Bands.csv, 10000
mesg  =   3,  c, eval (log.scans.scanPMEC_2.trigger), "{"type": 5, "hostname": "%(vars.unit)s", "message": "Rompimento de mascara em %(scan.start)s-%(scan.stop)s."}"

```

At the above example, one may highlight the following characteristics.

| Text in the example script | Explanation |
| --- | --- |
| `e = udp, 5555` | Create an UDP stream named `e` at port `5555`.|
| `timer = 1 sec` | Run this block every second. Since Zabbix, must not be kept waiting for a message (less the 2 seconds), the output stream needs to be updated very often and this value was chosen as a reasonable compromise, not increasing rfeye node CPU usage in more then one or two percent.
| `eval(log.streams.udpe.active)` | Function that evaluates TRUE when the UDP stream output is active, i.e. a client is connected. |
| `<json><\json>` | Tags used to mark the beginning and end of the json content to be processed by Zabbix or other clients. This method is used to avoid the need of more complex processing of the binary UDP stream as defined by CRFS specific format.
| `%(log.scans.scanPMRD_2.opns.peak1.runs)s` | Rfeye logger variable associated with the number of times the peak1 in the PMRD_2 run scan is executed |
| `%(log.scans.scanPMRD_2.opns.mesg.runs)s` | Rfeye logger variable associated with the number of times the message in the run scan is issued. This is a trick to count the number of times the level threshold defined in the mask is overcome |

Other useful variables that may be used:
| Text in the example script | Explanation |
| --- | --- |
| `log.scans.<scan>.start` | Scan start frequency. May be used instead of fixed frequency value to structure the JSON output. This may be used for automatically naming the bands. It was not considered relevant in the first implementation since any change in the json output will impact in the Zabbix template and processing rules, thus naming in the json output is perse irrelevant |
| `log.scans.<scan>.stop` | Scan stop frequency |

# JSON output

In this example, the JSON message that is sent for Zabbix processing is presented according to the following

```json
{
    "76_108MHz": {
        "scans": <#_SCAN_IN_THE_76_108_MHz_BAND>(int),
        "trigger":<#_OF_TIMES_MASK_WAS_TRIGGERED_IN_THE_76_108_MHz_BAND>(int)
    },
    "108_137MHz":{
        "scans": <#_SCAN_IN_THE_108_137_MHz_BAND>(int),
        "trigger":<#_OF_TIMES_MASK_WAS_TRIGGERED_IN_THE_108_137_MHz_BAND>(int)
    }
}
```

# Roadmap

This section presents a simplified view of the roadmap and knwon issues.

For more details, see the [open issues](https://github.com/FSLobao/RF.Fuse/issues) 

* [x] build test configuration
* [x] test configuration

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>

<!-- LICENSE -->
## License

Distributed under the GNU General Public License (GPL), version 3. See [`LICENSE.txt`](../../LICENSE).

For additional information, please check <https://www.gnu.org/licenses/quick-guide-gplv3.html>

This license model was selected with the idea of enabling collaboration of anyone interested in projects listed within this group.

It is in line with the Brazilian Public Software directives, as published at: <https://softwarepublico.gov.br/social/articles/0004/5936/Manual_do_Ofertante_Temporario_04.10.2016.pdf>

Further reading material can be found at:
* <http://copyfree.org/policy/copyleft>
* <https://opensource.stackexchange.com/questions/9805/can-i-license-my-project-with-an-open-source-license-but-disallow-commercial-use>
* <https://opensource.stackexchange.com/questions/21/whats-the-difference-between-permissive-and-copyleft-licenses/42#42>

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>


<!-- ACKNOWLEDGMENTS -->
## References

* [root page](/README.md)

<p align="right">(<a href="#indexerd-md-top">back to top</a>)</p>


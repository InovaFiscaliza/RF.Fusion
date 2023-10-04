#!/usr/bin/env python
"""
Shared functions for appCataloga scripts
"""
import sys
sys.path.append('/etc/appCataloga')

import config as k

# Class to compose warning messages
NO_WARNING_MSG = "none"

class warning_msg:
    """Class to compose warning messages

    Returns:
        void: Variable warning_msg is updated with the new warning message
    """
    
    def __init__(self) -> None:
        self.warning_msg = NO_WARNING_MSG
        
    def compose_warning(self, new_warning):
        if self.warning_msg == NO_WARNING_MSG:
            self.warning_msg = (f'Warning: {new_warning}')
        else:
            self.warning_msg = (f'{self.warning_msg}, {new_warning}')   

# Class to parse shell like configuration file into dictionary
import configparser

def parse_cfg(cfg_data=""):
    """Parse shell like configuration file into dictionary

    Args:
        daemon_cfg (str): Content from the configuration file.
        e.g. indexerD.cfg
        Defaults to "".

    Returns:
        _dict_: shell variables returned as pairs of key and value
    """    
    config = configparser.ConfigParser()
    config.read_srting(cfg_data)

    properties_dict = {}
    for section in config.sections():
        for key, value in config.items(section):
            properties_dict[key] = value

    return properties_dict

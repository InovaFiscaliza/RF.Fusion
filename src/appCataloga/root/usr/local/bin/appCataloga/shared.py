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

def parse_cfg(cfg_data="", root_level=True, line_number=0):
    """Parse shell like configuration file into dictionary


    Args:
        cfg_data (str): Content from the configuration file.
        e.g. indexerD.cfg
        Defaults to "".
        root_level (bool, optional): Flag to indicate if the call is in the root level. Defaults to True.
        line_number (int, optional): Line number where the parsing should start. Defaults to 0.

    Returns:
        dict: shell variables returned as pairs of key and value
        int: line number where the parsing stopped if call was not in the root_level
    """    
    
    config_str=cfg_data.decode(encoding='utf-8')
    
    config_list=config_str.splitlines()
    
    config_dict = {}
    while line_number<len(config_list):
        line = config_list[line_number]
        line_number += 1
        try:
            key, value = line.split("=")
            config_dict[key] = value
        except:
            try:
                key=line.split["["][1].split["]"][0]
                if root_level:
                    config_dict[key], line_number = parse_cfg(cfg_data=config_list, root_level=False, line_number=line_number)
                else:
                    return (config_dict, line_number)
            except:
                # ignore lines that do not follow the pattern
                pass

    return config_dict

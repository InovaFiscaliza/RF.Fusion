#!/usr/bin/env python
"""
Shared functions for appCataloga scripts
"""
import sys
sys.path.append('/etc/appCataloga')

import time

import config as k

# Class to compose warning messages
NO_MSG = "none"

class log:
    """Class to compose warning messages

    Returns:
        void: Variable warning_msg is updated with the new warning message
    """
    
    def __init__(self) -> None:
        self.log_msg = []
        self.warning_msg = []
        self.error_msg = []
        self.verbose = {"log":k.VERBOSE,
                        "warning":k.VERBOSE,
                        "error":k.VERBOSE}
        
    def entry(self, new_entry):
        self.log_msg.append((time.time(),new_entry))
        
        if self.verbose["log"]:
            print(new_entry)

    def warning(self, new_entry):
        self.warning_msg.append((time.time(),new_entry))
        
        if self.verbose["warning"]:
            print(new_entry) 
            
    def error(self, new_entry):
        self.error_msg.append((time.time(),new_entry))
        
        if self.verbose["error"]:
            print(new_entry)

    def dump_log(self):
        message = ', '.join([str(elem[1]) for elem in self.log_msg])
        
        return message

    def dump_warning(self):
        message = ', '.join([str(elem[1]) for elem in self.warning_msg])
        
        return message
        
    def dump_error(self):
        message = ', '.join([str(elem[1]) for elem in self.error_msg])
        return message
    
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
        # handle standard lines with variable value assignation
        try:
            key, value = line.split("=")
            try:
                # try to convert value to float
                config_dict[key] = float(value)
            except:
                # if not possible, keep value as string
                config_dict[key] = value
        except:
            # handle section lines    
            try:
                if line[0]=="[" and line[-1]=="]":
                    key = line[1:-1]
                    if root_level:
                        config_dict[key], line_number = parse_cfg(cfg_data=cfg_data, root_level=False, line_number=line_number)
                    else:
                        return (config_dict, line_number-1)
                else:
                    # ignore lines that do not assign values or define sections
                    pass
            except:
                # ignore empty lines
                pass
    
    # return according to the call level
    if root_level:
        return config_dict
    else:
        return (config_dict, line_number)

class argument:
    """Class to parse and store command-line arguments"""
    
    def __init__(self, log_input=log(), arg_input={}) -> None:
        self.log = log_input
        self.data = arg_input
        
    def parse(self, sys_arg=[]):
        """Get command-line arguments and parse into a request to the server"""
        
        # loop through the arguments list and set the value of the argument if it is present in the command line
        for i in range(1, len(sys_arg)):
            arg_in = sys_arg[i].split("=")
            if arg_in[0] in self.data.keys():
                # Get the data type from sef.data value
                data_type = type(self.data[arg_in[0]]["value"])
                
                # Set the argument value and set the "set" flag to True
                self.data[arg_in[0]]["value"] = data_type(arg_in[1])
                self.data[arg_in[0]]["set"] = True
            else:
                self.log.warning(f"Argument '{arg_in[0]}' not recognized, ignoring it")
            
        # loop through the arguments list and compose a warning message for each argument that was not set
        for arg in self.data.keys():
            if not self.data[arg]["set"]:
                self.log.warning(self.data[arg]["warning"])
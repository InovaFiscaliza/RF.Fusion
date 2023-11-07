#!/usr/bin/env python
"""
Shared functions for appCataloga scripts
"""
import sys
sys.path.append('/etc/appCataloga')
import os

from datetime import datetime

import config as k


class font:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    
# Class to compose warning messages
NO_MSG = "none"

class log:
    """Class to compose warning messages

    Returns:
        void: Variable warning_msg is updated with the new warning message
    """
    
    def __init__(self,verbose=k.LOG_VERBOSE,
                 target_screen=False, 
                 target_file=False, 
                 log_file_name = k.LOG_FILE):
        
        """Initialize log object

        Args:
            verbose (bool, optional): Set verbose level for debug, warning and error. Defaults to LOG_VERBOSE in config file.
            target_screen (bool, optional): Set the output target to screen. Defaults to False.
            target_file (bool, optional): Set the output target to file. Defaults to False.
            log_file_name (str, optional): Set the output file name. Defaults to LOG_FILE in config file.
        """        
        
        self.target_screen = target_screen
        self.target_file = target_file
        self.log_file_name = log_file_name
        
        self.log_msg = []
        self.warning_msg = []
        self.error_msg = []
        
        self.pid =  os.getpid()
                
        if isinstance(verbose,dict):
            self.verbose = verbose
            
        elif isinstance(verbose,bool):
            self.verbose = {"log":verbose,
                            "warning":verbose,
                            "error":verbose}
        else:
            self.verbose = {"log":False,
                            "wng":False,
                            "err":False}
            self.warning(f"Invalid verbose value '{verbose}'. Using default 'False'")
        
        if target_file:
            try:
                now = datetime.now()
                date_time = now.strftime("%Y/%m/%d %H:%M:%S")
                message = f"{date_time}: p.{self.pid}: Log started\n"
                
                self.log_file = open(log_file_name, "a")
                self.log_file.write(message)
                self.log_file.close()
                self.target_file = True
            except:
                self.target_file = False
                self.warning(f"Invalid log_file_name value '{log_file_name}'. Disabling file logging")
        
    def entry(self, new_entry):
        
        now = datetime.now()
        self.log_msg.append((now,self.pid,new_entry))
        
        if self.verbose["log"]:
            date_time = now.strftime("%Y/%m/%d %H:%M:%S")
            if self.target_file:
                message = f"{date_time} | p.{self.pid} | {new_entry}\n"
                self.log_file = open(self.log_file_name, "a")
                self.log_file.write(message)
                self.log_file.close()
        
            if self.target_screen:
                message = f"{font.OKGREEN}{date_time} | p.{self.pid} | {font.ENDC}{new_entry}"
                print(message)

    def warning(self, new_entry):
        
        now = datetime.now()
        self.warning_msg.append((now,self.pid,new_entry))
        
        if self.verbose["wng"]:
            date_time = now.strftime("%Y/%m/%d %H:%M:%S")
            if self.target_file:
                message = f"{date_time} | p.{self.pid} | {new_entry}\n"
                self.log_file = open(self.log_file_name, "a")
                self.log_file.write(message)
                self.log_file.close()
        
            if self.target_screen:
                message = f"{font.WARNING}{date_time} | p.{self.pid} | {font.ENDC}{new_entry}"
                print(new_entry)
                            
    def error(self, new_entry):
        
        now = datetime.now()
        self.error_msg.append((now,self.pid,new_entry))
        
        if self.verbose["err"]:
            date_time = now.strftime("%Y/%m/%d %H:%M:%S")
            if self.target_file:
                message = f"{date_time} | p.{self.pid} | {new_entry}\n"
                self.log_file = open(self.log_file_name, "a")
                self.log_file.write(message)
                self.log_file.close()
        
            if self.target_screen:
                message = f"{font.FAIL}{date_time} | p.{self.pid} | {font.ENDC}{new_entry}"
                print(message)

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
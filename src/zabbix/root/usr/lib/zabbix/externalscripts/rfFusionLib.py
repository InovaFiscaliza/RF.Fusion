#!/usr/bin/python3
"""
Common functions for the RF.Fusion project
"""

class warning_msg:
    """Class to compose warning messages"""
    
    NO_WARNING_MSG = "none"
    
    def __init__(self) -> None:
        self.warning_msg = self.NO_WARNING_MSG

    def compose_warning(self, new_warning):
        if self.warning_msg == self.NO_WARNING_MSG:
            self.warning_msg = f"Warning: {new_warning}"
        else:
            self.warning_msg = f"{self.warning_msg}; {new_warning}"

    def is_set(self):
        if self.warning_msg == self.NO_WARNING_MSG:
            return True
        else:
            return False

class argument:
    """Class to parse and store command-line arguments"""
    
    def __init__(self, wm_input=warning_msg(), arg_input={}) -> None:
        self.wm = wm_input
        self.data = arg_input
        
    def parse(self, sys_arg=[]):
        """Get command-line arguments and parse into a request to the server"""
        
        # loop through the arguments list and set the value of the argument if it is present in the command line
        for i in range(1, len(sys_arg)):
            arg_in = sys_arg[i].split("=")
            if arg_in[0] in self.data.keys():
                self.data[arg[0]]["value"] = arg[1]
                self.data[arg[0]]["set"] = True
            else:
                self.wm.compose_warning(f"Argument '{arg[0]}' not recognized, ignoring it")
            
        # loop through the arguments list and compose a warning message for each argument that was not set
        for arg in self.data.keys():
            if not self.data[arg]["set"]:
                self.wm.compose_warning(self.data[arg]["warning"])
        
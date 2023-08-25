#!/usr/bin/env python
"""
Shared functions for appCataloga scripts
"""
import sys
sys.path.append('/etc/appCataloga')

class warning_msg:
    def __init__(self) -> None:
        self.warning_msg = "none"
        
    def compose_warning(self, new_warning):
        if self.warning_msg == NO_WARNING_MSG:
            self.warning_msg = (f'Warning: {new_warning}')
        else:
            self.warning_msg = (f'{self.warning_msg}, {new_warning}')   


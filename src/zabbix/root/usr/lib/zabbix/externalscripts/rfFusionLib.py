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
                # Get the data type from sef.data value
                data_type = type(self.data[arg_in[0]]["value"])
                
                # Set the argument value and set the "set" flag to True
                self.data[arg_in[0]]["value"] = data_type(arg_in[1])
                self.data[arg_in[0]]["set"] = True
            else:
                self.wm.compose_warning(f"Argument '{arg_in[0]}' not recognized, ignoring it")
            
        # loop through the arguments list and compose a warning message for each argument that was not set
        for arg in self.data.keys():
            if not self.data[arg]["set"]:
                self.wm.compose_warning(self.data[arg]["warning"])

def receive_message(client_socket, encoding="ISO-8859-1", buffer_size=16384, start_tag="<json>",end_tag="</json>", timeout=5):
    """Receive a message from the server

    Args:
        client_socket (_type_): socket object
        encoding (str, optional): enconding to be used to retrieve message from binnary data. Defaults to "ISO-8859-1".
        buffer_size (int, optional): size of each block to be read from the socket buffer. Defaults to 16384.
        start_tag (str, optional): tag that marks the beginning of the desired content. Defaults to "<json>".
        end_tag (str, optional): tag that marks the end of the desired content. Defaults to "</json>".
        timeout (int, optional): time to wait for data to complete. Defaults to 5.

    Returns:
        (str): content between start_tag and end_tag
    """    """"""
    import time
    
    """Receive a message from the server"""
    start_receiving_message_time = time.time()
    decoded_response = ""
    receiving_message = True
    try:
        while receiving_message:
            response = client_socket.recv(buffer_size)
            # decode the bytestring
            try:
                # merge the response with the tail of the previous response
                decoded_response = decoded_response + response.decode(encoding)
                
            except Exception as e:
                print(f'{{"Status":0,"Message":"Error decoding binary data: {e}"}}')
                client_socket.close()
                exit()

            # find the end tags in the response
            end_index = decoded_response.lower().rfind(end_tag)
            
            # if end_index is different from -1 and timeout has not been reached, then the message is complete
            if (end_index !=-1):
                receiving_message = False
                client_socket.close()
            
            if (time.time() - start_receiving_message_time > timeout):
                print(f'{"Status":0,"Message":"Error: Incomplete JSON received. Dumped: {response}"}')
                client_socket.close()
                exit()

    except Exception as e:
        print(f'{{"Status":0,"Message":"Error while receiving data: {e}"}}')
        client_socket.close()
        exit()

    # find the start and end tags in the response. May capture spurious messages from the server before the JSON data starts
    start_index = decoded_response.lower().rfind(start_tag)
    
    # extract JSON data removing the last bracket to later splice with the tail json data from this script
    output = decoded_response[start_index + len(start_tag) : end_index]

    return output

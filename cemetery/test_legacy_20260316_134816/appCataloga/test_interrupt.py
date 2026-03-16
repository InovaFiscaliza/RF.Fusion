import signal
import time
import inspect

def exit_gracefully():
    global counter_function
    global counter_class  # Declare counter as global
    global keep_going

    cleanup()
    counter_class.delete()
    keep_going = False

# Define a signal handler for SIGTERM
def sigterm_handler(signal=None, frame=None):    
    current_function = inspect.currentframe().f_back.f_code.co_name
    print(f"\nSIGTERM received at {current_function}")
    exit_gracefully()

def sigint_handler(signal=None, frame=None):    
    current_function = inspect.currentframe().f_back.f_code.co_name
    print(f"\nSIGINT received at {current_function}")
    exit_gracefully()

# Register the signal handler
signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)

# Define a cleanup function
def cleanup():
    global counter_function
    
    counter_function = 0
    print("Reseting counter function")

def increase_counter(counter):
    counter += 1
    print(f"Counter Function: {counter}")
    time.sleep(1)  # Sleep for 1 second before the next iteration
    return counter

class Counter:
    def __init__(self, counter):
        self.counter = counter

    def __call__(self):
        self.counter += 1
        print(f"Counter Class: {self.counter}")
        time.sleep(1)  # Sleep for 1 second before the next iteration
        return self.counter
    
    def delete(self):
        self.counter = 0
        print("Reseting Counter class")

# Main loop
def main():
    global counter_function
    global counter_class
    global keep_going
    
    while keep_going:
        counter_function = increase_counter(counter_function)
        
        counter_class()
        
        time.sleep(1)  # Sleep for 1 second before the next iteration
        print("Looping...")

if __name__ == "__main__":
    counter_function = 0
    counter_class = Counter(0)
    keep_going = True
    
    main()

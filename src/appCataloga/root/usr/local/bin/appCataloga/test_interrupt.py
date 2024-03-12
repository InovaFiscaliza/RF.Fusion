import signal
import sys
import time

# Define a signal handler for SIGTERM
def sigterm_handler(signal, frame):
    global counter  # Declare counter as global
    print(f"\nSIGTERM received. Exiting. Exiting when counter is {counter}")
    counter = cleanup(counter)
    print(f"Now counter is {counter}")
    sys.exit(0)


# Define a cleanup function
def cleanup(counter):
    print(f"Performing cleanup with counter value: {counter}")
    return 0


# Register the signal handler
signal.signal(signal.SIGTERM, sigterm_handler)

# Main loop
try:
    counter = 0
    while True:
        counter += 1
        print(f"Looping... Counter: {counter}")
        time.sleep(1)  # Sleep for 1 second before the next iteration

except KeyboardInterrupt:
    # Handle Ctrl+C
    print(f"\nKeyboardInterrupt received. Exiting when counter is {counter}")
    counter = cleanup(counter)
    print(f"Now counter is {counter}")
    sys.exit(0)


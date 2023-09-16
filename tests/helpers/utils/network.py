import random
import socket
import threading


def is_port_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(('localhost', port))
            s.close()
            return True
        except OSError:
            return False


MIN_PORT = 20000
MAX_PORT = 40000

port_cursor = random.choice(range(MIN_PORT, MAX_PORT, 10))

g_lock = threading.Lock()


def get_free_port():
    global port_cursor
    global g_lock
    with g_lock:
        while True:
            port_cursor += 1
            if port_cursor == MAX_PORT:
                port_cursor = MIN_PORT

            if is_port_available(port_cursor):
                return port_cursor


__all__ = [
    "is_port_available",
    "get_free_port",
]

if __name__ == '__main__':
    # test
    for i in range(10):
        print(get_free_port())

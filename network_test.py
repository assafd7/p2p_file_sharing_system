import socket
import sys
import threading

USAGE = '''
Usage:
  python network_test.py server <port>
  python network_test.py client <server_ip> <port>
'''

def run_server(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', port))
    s.listen(1)
    print(f'[SERVER] Listening on 0.0.0.0:{port}...')
    while True:
        conn, addr = s.accept()
        print(f'[SERVER] Connection from {addr}')
        data = conn.recv(1024)
        print(f'[SERVER] Received: {data!r}')
        conn.sendall(b'ACK')
        conn.close()
        print('[SERVER] Connection closed')

def run_client(server_ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        print(f'[CLIENT] Connecting to {server_ip}:{port}...')
        s.connect((server_ip, port))
        print('[CLIENT] Connected! Sending test message...')
        s.sendall(b'Hello from client!')
        data = s.recv(1024)
        print(f'[CLIENT] Received: {data!r}')
        print('[CLIENT] Success!')
    except Exception as e:
        print(f'[CLIENT] Connection failed: {e}')
    finally:
        s.close()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(USAGE)
        sys.exit(1)
    mode = sys.argv[1]
    if mode == 'server' and len(sys.argv) == 3:
        run_server(int(sys.argv[2]))
    elif mode == 'client' and len(sys.argv) == 4:
        run_client(sys.argv[2], int(sys.argv[3]))
    else:
        print(USAGE)
        sys.exit(1) 
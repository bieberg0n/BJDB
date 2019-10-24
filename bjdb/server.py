import socket
from threading import Thread
import bjdb


def log(*args):
    print(*args)


def recv(conn, n):
    data = conn.recv(n)
    while len(data) < n:
        data += conn.recv(n - len(data))

    return data.decode()


def recv_until_return(conn):
    data = b''
    while len(data) < 2 or data[-2:] != b'\r\n':
        data += conn.recv(1)

    return data[:-2].decode()


def recv_argv(conn):
    flag = recv(conn, 1)
    assert flag == '$'
    # conn.close()
    # return

    argv_len = int(recv_until_return(conn))
    argv = recv(conn, argv_len)

    assert recv(conn, 2) == '\r\n'
    # conn.close()
    # return

    return argv


def recv_argvs(conn, argv_count):
    return [recv_argv(conn) for _ in range(argv_count)]


def parse(conn):
    flag = recv(conn, 1)
    assert flag == '*'

    argv_count = int(recv_until_return(conn))
    argvs = recv_argvs(conn, argv_count)
    log(argvs)
    return argvs


class Server:
    def __init__(self):
        self.db = bjdb.BJDB('bj.db')
        self.op_map = {
            'CREATE TABLE': self.db.create_table,
            'INSERT': self.db.insert,
            'SEARCH': self.db.search,
        }

    def handle(self, conn):
        while True:
            try:
                argvs = parse(conn)
            except AssertionError as e:
                log(e)
                conn.close()
                return

            op = argvs[0]
            if not self.op_map.get(op):
                conn.close()
                return

    def run(self):
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('127.0.0.1', 6380))
        s.listen(5)

        # while True:
        conn, addr = s.accept()
        log(addr, 'connect in')
        Thread(target=self.handle, args=(conn,)).start()


def main():
    serv = Server()
    serv.run()


if __name__ == '__main__':
    main()

import http.server
import os
from os import environ
import duckdb
from urllib.parse import urlparse

PORT = int(environ['DUCKDB_SERVER_PORT'])

class Handler(http.server.BaseHTTPRequestHandler):
    sessions: dict[str, duckdb.DuckDBPyConnection] = {}

    def read_input(self):
        len = int(self.headers.get('Content-Length', "0"))
        return self.rfile.read(len)
        
    def do_POST(self):
        if self.path.startswith("/upsert-session"):
            session_id = int(self.read_input())
            if session_id in self.sessions:
                self.sessions[session_id].close()
            self.sessions[session_id] = duckdb.connect(':memory:')
            self.send_response(200, message='OK')
            self.end_headers()
        elif self.path.startswith("/exec-sql"):
            query = urlparse(self.path).query
            query_components = dict(qc.split("=") for qc in query.split("&"))
            session_id = int(query_components['session_id'])
            statement = self.read_input()
            try:
                results = duckdb.from_query(statement, connection=self.sessions[session_id])
                self.send_response(200)
                self.send_header('Content-Type', 'text/csv')
                self.end_headers()
                if results is not None:
                    results.write_csv('out.csv', header=True)
                    with open("out.csv", "r") as f:
                        for line in f.readlines():
                            self.wfile.write(bytes(line, 'UTF-8'))
                            self.wfile.write(bytes('\n', 'UTF-8'))
                    os.remove("out.csv")
            except duckdb.Error as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(bytes(repr(e), 'UTF-8'))
        else:
            self.send_response(400, message="invalid command: available are /upsert-session and /exec-sql routes")
            self.end_headers()

with http.server.HTTPServer(("0.0.0.0", PORT), Handler) as httpd:
    print("serving at port", PORT)
    httpd.serve_forever()
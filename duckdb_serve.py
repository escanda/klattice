import http.server
import os
from os import environ
import duckdb
from urllib.parse import urlparse
import random

PORT = int(environ['DUCKDB_SERVER_PORT'])

class Handler(http.server.BaseHTTPRequestHandler):
    MAX_ID_LOOP_TIMES = 4
    sessions: dict[str, duckdb.DuckDBPyConnection] = {}

    def read_input(self):
        len = int(self.headers.get('Content-Length', "0"))
        return self.rfile.read(len)

    def get_session_id(self):
        query = urlparse(self.path).query
        query_components = dict(qc.split("=") for qc in query.split("&"))
        session_id = int(query_components['session_id'])
        return session_id

    def dump(self, rel: duckdb.DuckDBPyRelation):
        self.send_response(200)
        self.send_header('Content-Type', 'text/csv')
        self.end_headers()
        if rel is not None:
            rel.write_csv('out.csv', header=True)
            with open("out.csv", "r") as f:
                for line in f.readlines():
                    self.wfile.write(bytes(line, 'UTF-8'))
                    self.wfile.write(bytes('\n', 'UTF-8'))
                os.remove("out.csv")
            
    def do_POST(self):
        if self.path.startswith("/make-session"):
            times = 0
            a_sid = -1
            while times < self.MAX_ID_LOOP_TIMES:
                a_sid = random.choice(range(1000))
                if not a_sid in self.sessions:
                    break
                times += 1
            if a_sid > -1:
                self.send_response(200)
                self.end_headers()
                self.sessions[a_sid] = duckdb.connect(':memory:')
                print("made available session with id %d" % (a_sid))
                self.wfile.write(bytes(str(a_sid), 'ASCII'))
            else:
                self.send_response(500, message='Cannot figure out random session id')
        elif self.path.startswith("/upsert-session"):
            session_id = int(self.read_input())
            if session_id in self.sessions:
                print("there was a previous session by id %d" % (session_id,))
                self.sessions[session_id].close()
            print("opening session by id %d" % (session_id,))
            self.sessions[session_id] = duckdb.connect(':memory:')
            self.send_response(200, message='OK')
            self.end_headers()
            self.wfile.write(bytes(str(session_id), 'ASCII'))
        elif self.path.startswith("/exec-sql"):
            session_id = self.get_session_id()
            statement = self.read_input()
            print("executing statement in session %d '%s'" % (session_id, statement))
            try:
                rel = duckdb.from_query(statement, connection=self.sessions[session_id])
                self.dump(rel)
            except duckdb.Error as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(bytes(repr(e), 'UTF-8'))
        elif self.path.startswith("/exec-substrait"):
            session_id = self.get_session_id()
            payload = self.read_input()
            print("executing Substrait payload in session %d" % (session_id,))
            rel = duckdb.from_substrait(payload, connection=self.sessions[session_id])
            self.dump(rel)
        elif self.path.startswith("/exec-arbitrary"):
            session_id = self.get_session_id()
            query = self.read_input()
            print("executing arbitrary statement in session id %d: '%s'" % (session_id, query))
            self.sessions[session_id].execute(query)
            self.send_response(200)
            self.end_headers()
        else:
            self.send_response(400, message="invalid command: available are /upsert-session or /exec-sql or /exec-substrait routes")
            self.end_headers()

with http.server.HTTPServer(("0.0.0.0", PORT), Handler) as httpd:
    print("serving at port", PORT)
    httpd.serve_forever()
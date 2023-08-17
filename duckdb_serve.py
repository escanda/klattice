import http.server
from os import environ
import duckdb

PORT = int(environ['DUCKDB_SERVER_PORT'])

class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers['Content-Length'])
        statement = self.rfile.read(length)
        try:
            result = duckdb.sql(statement)
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv')
            self.end_headers()
            if result is not None:
                result.write_csv('out.csv', header=True)
                with open("out.csv", "r") as f:
                    csv_contents = "\n".join(f.readlines())
                    self.wfile.write(bytes(csv_contents, 'UTF-8'))
        except duckdb.Error as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(bytes(repr(e), 'UTF-8'))

with http.server.HTTPServer(("0.0.0.0", PORT), Handler) as httpd:
    print("serving at port", PORT)
    httpd.serve_forever()
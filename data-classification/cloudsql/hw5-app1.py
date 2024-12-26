import http.server
import socketserver
from google.cloud import storage, pubsub_v1, logging
import os
from google.cloud.sql.connector import Connector, IPTypes
import pymysql
import socket, struct
import sqlalchemy
from datetime import datetime

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
logging_client = logging.Client()

log_name = "fetch-files-log"
logger = logging_client.logger(log_name)

PROJECT_ID="ds-561-mohitsai"
DB_USER="root"
DB_PASS="CloudComputing"
DB_NAME="hw5"
INSTANCE_CONNECTION_NAME= "ds-561-mohitsai:us-central1:ds561-hw5"

PUBSUB_TOPIC = "projects/ds-561-mohitsai/topics/banned-country-topic"

banned_countries = ["North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"]

# initialize Connector object
connector = Connector()

# getconn now set to private IP
def getconn():
    conn = connector.connect(
      INSTANCE_CONNECTION_NAME, # ::
      "pymysql",
      user=DB_USER,
      password=DB_PASS,
      db=DB_NAME,
      ip_type=IPTypes.PUBLIC
    )
    print("Database connection established.")
    return conn

# create connection pool
pool = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
)

# Function to check and create tables if they don't exist
def create_tables_if_not_exists():
    with pool.connect() as connection:
        # SQL statements to create tables if they do not exist
        connection.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS requests (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country VARCHAR(255) NOT NULL,
                client_ip VARCHAR(45),
                gender ENUM('Male', 'Female'),
                age_group VARCHAR(20),
                income_group VARCHAR(20),
                is_banned BOOLEAN,
                request_time DATETIME,
                requested_file VARCHAR(255)
            );
        """))
        connection.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS failed_requests (
                id INT AUTO_INCREMENT PRIMARY KEY,
                request_time DATETIME,
                requested_file VARCHAR(255),
                error_code INT
            );
        """))

# Call the function once at the start of the server to ensure tables are created
create_tables_if_not_exists()

class MyHttpRequestHandler(http.server.SimpleHTTPRequestHandler):

    def do_GET(self):
        request_path = self.path.strip('/')
        path_parts = request_path.split('/')
        if len(path_parts) < 3:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Invalid path format. Expected /<bucket_name>/<directory>/<file_name>")
            return

        bucket_name = path_parts[0]
        file_directory = '/'.join(path_parts[1:-1])
        file_name = path_parts[-1]
        country = self.headers.get('X-country')
        client_ip = self.headers.get('X-client-IP')
        gender = self.headers.get('X-gender')
        age_group = self.headers.get('X-age')
        income_group = self.headers.get('X-income')
        request_time = datetime.now()

        # Log the received headers
        # print(f"Received request for: {file_name}")
        # print(f"Country: {country}, Client IP: {client_ip}, Gender: {gender}, Age Group: {age_group}, Income Group: {income_group}")

        if country in banned_countries:
            self.log_error("Access denied from %s", country)
            send_banned_request_to_pubsub(country)
            self.send_response(400)
            self.end_headers()
            self.wfile.write(f"Access denied for requests from {country}".encode())

            # Log banned request to failed_requests table
            with pool.connect() as connection:
                # print("Logging banned request...")
                try:
                    connection.execute(
                        sqlalchemy.text("INSERT INTO failed_requests (request_time, requested_file, error_code) VALUES (:request_time, :requested_file, :error_code)"),
                        {"request_time": request_time, "requested_file": file_name, "error_code": 400}
                    )
                    connection.commit()
                    connection.execute(
                        sqlalchemy.text("""
                            INSERT INTO requests (country, client_ip, gender, age_group, income_group, is_banned, request_time, requested_file)
                            VALUES (:country, :client_ip, :gender, :age_group, :income_group, :is_banned, :request_time, :requested_file)
                        """),
                        {
                            "country": country,
                            "client_ip": client_ip,
                            "gender": gender,
                            "age_group": age_group,
                            "income_group": income_group,
                            "is_banned": True,
                            "request_time": request_time,
                            "requested_file": file_name
                        }
                    )
                    connection.commit()
                    # print("Banned request logged.")
                except Exception as e:
                    print(f"Error logging banned request: {e}")
            return

        try:
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(f'{file_directory}/{file_name}')

            if not blob.exists():
                self.log_error("File %s not found", file_name)
                self.send_response(404)
                self.end_headers()
                self.wfile.write(f"File {file_name} not found.".encode())

                # Log missing file request to failed_requests table
                with pool.connect() as connection:
                    # print("Logging failed request for missing file...")
                    try:
                        connection.execute(
                            sqlalchemy.text("INSERT INTO failed_requests (request_time, requested_file, error_code) VALUES (:request_time, :requested_file, :error_code)"),
                            {"request_time": request_time, "requested_file": file_name, "error_code": 404}
                        )
                        connection.commit()
                        # print("Failed request for missing file logged.")
                    except Exception as e:
                        print(f"Error logging failed request for missing file: {e}")
                return

            file_content = blob.download_as_text()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(file_content.encode())

            # Log successful request to requests table
            with pool.connect() as connection:
                # print("Logging successful request...")
                try:
                    connection.execute(
                        sqlalchemy.text("""
                            INSERT INTO requests (country, client_ip, gender, age_group, income_group, is_banned, request_time, requested_file)
                            VALUES (:country, :client_ip, :gender, :age_group, :income_group, :is_banned, :request_time, :requested_file)
                        """),
                        {
                            "country": country,
                            "client_ip": client_ip,
                            "gender": gender,
                            "age_group": age_group,
                            "income_group": income_group,
                            "is_banned": False,
                            "request_time": request_time,
                            "requested_file": file_name
                        }
                    )
                    connection.commit()
                    # print("Successful request logged.")
                except Exception as e:
                    print(f"Error logging successful request: {e}")

        except Exception as e:
            self.log_error("Error: %s", str(e))
            self.send_response(500)
            self.end_headers()
            self.wfile.write(f"Error: {str(e)}".encode())

            # Log server error to failed_requests table
            with pool.connect() as connection:
                # print("Logging server error...")
                try:
                    connection.execute(
                        sqlalchemy.text("INSERT INTO failed_requests (request_time, requested_file, error_code) VALUES (:request_time, :requested_file, :error_code)"),
                        {"request_time": request_time, "requested_file": file_name, "error_code": 500}
                    )
                    connection.commit()
                    # print("Server error logged.")
                except Exception as e:
                    print(f"Error logging server error: {e}")

    def do_POST(self):
        self.send_error(501, "Method POST not implemented")

    def do_PUT(self):
        self.send_error(501, "Method PUT not implemented")

    def do_DELETE(self):
        self.send_error(501, "Method DELETE not implemented")

    def do_OPTIONS(self):
        self.send_error(501, "Method OPTIONS not implemented")

    def do_HEAD(self):
        self.send_error(501, "Method HEAD not implemented")

    def log_error(self, format, *args):
        message = format % args
        logger.log_text(message, severity='ERROR')

def send_banned_request_to_pubsub(country):
    try:
        message = f"Banned request from {country}".encode("utf-8")
        publisher.publish(PUBSUB_TOPIC, message)
    except Exception as e:
        logger.log_text(f"Failed to publish to Pub/Sub: {str(e)}", severity='ERROR')


if __name__ == "__main__":
    PORT = 8081
    with socketserver.TCPServer(("", PORT), MyHttpRequestHandler) as httpd:
        print(f"Serving on port {PORT}")
        httpd.serve_forever()
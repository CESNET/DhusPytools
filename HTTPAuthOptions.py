import requests
from requests.auth import HTTPBasicAuth
from requests.auth import AuthBase
import logging
import netrc
from urllib.parse import urlparse

# Custom authentication class for Bearer Token
class HTTPBearerAuth(AuthBase):
    def __init__(self, token_file=".token"):
        logging.debug(f"Initializing {type(self)}")
        self.token = self._read_token(token_file)

    def _read_token(self, token_file):
        """Reads the token from a file."""
        try:
            logging.debug(f"Reading file {token_file}")
            with open(token_file, "r") as f:
                return f.read().strip()
        except FileNotFoundError:
            logging.error(f"Token file '{token_file}' not found.")
            raise

    def __call__(self, r):
        """Attach the Bearer token to the request headers."""
        r.headers["Authorization"] = f"Bearer {self.token}"
        #r.headers["Accept"] = "application/json"
        return r

class FileBasedBasicAuth(HTTPBasicAuth):
    def __init__(self, filepath=".basic-auth"):
        """
        Initializes the authentication object by reading credentials from the file.

        :param filepath: Path to the .basic-auth file (default: ".basic-auth")
        """
        logging.debug(f"Initializing {type(self)}")
        username, password = self._read_credentials(filepath)
        super().__init__(username, password)

    def _read_credentials(self, filepath):
        """Reads user:password from the specified file."""
        try:
            with open(filepath, "r") as f:
                line = f.readline().strip()  # Read first line and strip whitespace
                if ":" not in line:
                    raise ValueError("Invalid format: Expected 'user:password'")
                
                return line.split(":", 1)  # Split at the first colon
            
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {filepath}")
        except Exception as e:
            raise RuntimeError(f"Error reading {filepath}: {e}")

class KeycloakTokenAuth(HTTPBearerAuth):
    def __init__(self, server_url, realm, client_id, client_secret=None, netrc_file=None):
        logging.debug(f"Initializing {type(self)}")
        self.server_url = server_url
        self.realm = realm
        self.client_id = client_id
        self.client_secret = client_secret
        self.netrc_file = netrc_file
        self.token_url = f"{server_url}/realms/{realm}/protocol/openid-connect/token"
        username, password = self._read_credentials()
        self.token = self._get_token(username, password)

    def _read_credentials(self):
        n = netrc.netrc(self.netrc_file)
        host = urlparse(self.server_url).netloc
        creds = n.authenticators(host)
        return creds[0], creds[2]

    def _get_token(self, username, password):
        data = {
            "grant_type": "password",
            "client_id": self.client_id,
            "username": username,
            "password": password,
        }

        if self.client_secret:
            data["client_secret"] = self.client_secret

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.token_url, data=data, headers=headers)

        if response.status_code == 200:
            access_token = response.json()["access_token"]
            #logging.debug(f"Got access token from {self.token_url} {access_token}")
            return access_token
        else:
            raise Exception(f"Failed to get token: {response.status_code}, {response.text}")

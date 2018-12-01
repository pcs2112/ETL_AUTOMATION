"""Application configuration."""
import os
from dotenv import load_dotenv

# load dotenv
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Set the settings
config = {
	'DB_SERVER': os.getenv('DB_HOST'),
	'DB_NAME': os.getenv('DB_NAME'),
	'DB_USER': os.getenv('DB_USER'),
	'DB_PASSWORD': os.getenv('DB_PASSWORD'),
	'DB_DRIVER': os.getenv('DB_DRIVER'),
	'DB_TRUSTED_CONNECTION': os.getenv('DB_TRUSTED_CONNECTION') == 1,
}


def get_config():
	return config

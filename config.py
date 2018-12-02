"""Application configuration."""
import os
from dotenv import load_dotenv

ROOT_DIR = os.path.join(os.path.dirname(__file__))

# load dotenv
dotenv_path = os.path.join(ROOT_DIR, '.env')
load_dotenv(dotenv_path)

# ETL config dirs
ETL_CONFIG_IN_DIR = os.path.join(ROOT_DIR, 'in')
ETL_CONFIG_OUT_DIR = os.path.join(ROOT_DIR, 'out')

# Code generation dirs
CODE_GENERATION_IN_DIR = os.path.join(ROOT_DIR, 'code_generation/in')
CODE_GENERATION_OUT_DIR = os.path.join(ROOT_DIR, 'code_generation/out')

# Set the settings
config = {
	'DB_SERVER': os.getenv('DB_HOST'),
	'DB_NAME': os.getenv('DB_NAME'),
	'DB_USER': os.getenv('DB_USER'),
	'DB_PASSWORD': os.getenv('DB_PASSWORD'),
	'DB_DRIVER': os.getenv('DB_DRIVER'),
	'DB_TRUSTED_CONNECTION': os.getenv('DB_TRUSTED_CONNECTION') == 1,
	'ROOT_DIR': ROOT_DIR,
	'ETL_CONFIG_IN_DIR': ETL_CONFIG_IN_DIR,
	'ETL_CONFIG_OUT_DIR': ETL_CONFIG_OUT_DIR,
	'CODE_GENERATION_IN_DIR': CODE_GENERATION_IN_DIR,
	'CODE_GENERATION_OUT_DIR': CODE_GENERATION_OUT_DIR
}


def get_config():
	return config

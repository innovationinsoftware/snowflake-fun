"""Local Python demo settings. Never save passwords in this file."""
from getpass import getpass
import os

_account = os.environ.get('SNOWFLAKE_ACCOUNT') or input('Account (organization-account): ').strip()
_username = os.environ.get('SNOWFLAKE_USER') or input('Snowflake username: ').strip()
_pwd = getpass('Snowflake password: ')

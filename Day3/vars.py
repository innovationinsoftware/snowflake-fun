"""Shared local credentials for the Python demos. Never commit .env files."""
from getpass import getpass
from pathlib import Path
import os

from dotenv import dotenv_values


def get_credentials():
    env_path = Path(__file__).resolve().parent / '.env'
    # Passwords containing ${...} must be read literally, without expansion.
    saved = dotenv_values(env_path, interpolate=False) if env_path.exists() else {}
    values = {}
    for key, prompt in (
        ('SNOWFLAKE_ACCOUNT', 'Account (organization-account): '),
        ('SNOWFLAKE_USER', 'Snowflake username: '),
        ('SNOWFLAKE_PASSWORD', 'Snowflake password: '),
    ):
        value = os.environ.get(key) or saved.get(key)
        while not value:
            value = getpass(prompt) if key.endswith('PASSWORD') else input(prompt).strip()
        values[key] = value

    if not env_path.exists():
        answer = input('Save credentials in local .env (plain-text password)? [y/N]: ')
        if answer.strip().lower() in ('y', 'yes'):
            # Exclusive creation avoids overwriting an existing file. POSIX mode
            # restricts access; Windows uses the folder's inherited permissions.
            def quote(value):
                return "'" + value.replace('\\', '\\\\').replace("'", "\\'") + "'"
            content = '# Local credentials. Do not share or commit this file.\n'
            content += ''.join(f'{key}={quote(value)}\n' for key, value in values.items())
            try:
                fd = os.open(env_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                    stream.write(content)
                print('Saved local .env beside the demo scripts.')
            except OSError:
                print('Could not save .env; continuing with credentials for this run only.')

    return values['SNOWFLAKE_ACCOUNT'], values['SNOWFLAKE_USER'], values['SNOWFLAKE_PASSWORD']


_account, _username, _pwd = get_credentials()

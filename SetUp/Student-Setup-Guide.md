# Snowflake Fundamentals — Student Setup Guide

> **Course:** Snowflake Fundamentals 4-Day Class
> **Copyright © 2026 Innovation In Software Corporation. All rights reserved.**

## Table of Contents

1. [Overview](#1-overview)
2. [Snowflake Account](#2-snowflake-account)
3. [Snowflake CLI Installation](#3-snowflake-cli-installation)
4. [Firewall & Network Requirements](#4-firewall--network-requirements)
5. [Named Connection & First Query](#5-named-connection--first-query)
6. [Lab Files Setup](#6-lab-files-setup)
7. [Pre-Installed VM Option](#7-pre-installed-vm-option)
8. [Verification Checklist](#8-verification-checklist)
9. [Troubleshooting](#9-troubleshooting)

## 1. Overview

Complete this setup before Day 1. You need a Snowflake account, Snowflake CLI,
a browser, and a local copy of the lab files. Snowflake CLI uses the `snow`
command. SnowSQL is a legacy client and is not required for these labs.

**Run commands beginning with `snow` in a local terminal** (PowerShell,
Command Prompt, or macOS/Linux Terminal). Run SQL through `snow sql` or in
Snowsight, as directed. Local file transfers with `PUT` and `GET` use the CLI
in this course; do not paste those commands into a Snowsight SQL editor.

## 2. Snowflake Account

### 2.1 Account access

Use the account supplied by the instructor, or create a trial at
[Snowflake signup](https://signup.snowflake.com/). Select Enterprise edition
for the class features, and use the cloud/region specified by the instructor.
Activate the account and confirm you can sign in to Snowsight.

### 2.2 Connection details

In Snowsight, open the account details and copy the account identifier in
`organization-account` format. Do not use the entire browser URL as the CLI
account value. Record your username and the class role and warehouse.

The labs use `SYSADMIN` and `COMPUTE_WH` where available. The instructor must
ensure your user has the class role and that the role has warehouse access.
Use administrative privileges only for setup steps that require them.

Confirm whether you will use a Snowflake password or the organization's SSO.
Complete MFA when required by the account. Do not assume that a trial account
has the same authentication policy as a corporate account.

## 3. Snowflake CLI Installation

Use the [official installation guide](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation)
to select the current installer for your operating system and architecture.
On managed computers, ask IT to install the tool before class.

| Platform | Installation approach |
|----------|-----------------------|
| Windows | Download and run the Windows installer linked from the official guide. Open a new terminal after installation. |
| macOS | Use the macOS package installer or the Homebrew instructions in the official guide. |
| Linux | Use the appropriate DEB or RPM package and its documented installation steps. |

An alternative for an environment that already has `pipx` and a supported
Python runtime is:

```text
pipx install snowflake-cli
```

Verify installation in a new terminal:

```text
snow --version
snow --help
```

Expected: a version number and the Snowflake CLI command help. Use the upgrade
procedure for your installation method; do not assume automatic upgrades.
For a `pipx` installation, use `pipx upgrade snowflake-cli`.

## 4. Firewall & Network Requirements

### 4.1 Account-specific endpoints

For corporate networks or VMs, have IT approve the account's service,
stage-storage, and certificate-validation endpoints. In Snowsight, run:

```sql
SELECT SYSTEM$ALLOWLIST();
```

For private connectivity, ask the administrator for the appropriate private
endpoint list. Give IT the returned hostnames and ports rather than a generic
cloud-storage wildcard list. Most service and file-transfer traffic uses
outbound HTTPS on port 443; certificate-validation endpoints can require
port 80. Normal client connections do not require an inbound listener.

Installation also requires access to the selected download source. Depending
on the installation method, this may include Snowflake's package repository,
GitHub, PyPI, or the organization's software mirror. SSO/MFA can require
additional identity-provider endpoints.

### 4.2 Proxy settings

If IT requires a proxy, use its supplied settings. For example, in PowerShell:

```powershell
$env:HTTPS_PROXY = "http://proxy.yourcompany.com:8080"
$env:HTTP_PROXY = "http://proxy.yourcompany.com:8080"
```

In Command Prompt, use `set HTTPS_PROXY=...` and `set HTTP_PROXY=...`.
On macOS/Linux, use `export HTTPS_PROXY=...` and `export HTTP_PROXY=...`.
Ask IT which hosts should bypass the proxy through `NO_PROXY`.

For certificate or OCSP errors, have IT check the endpoints, trust chain,
proxy, and system clock. Do not disable certificate checks as a setup step.
A successful web request alone does not verify login or stage transfers:
complete the connection test below and the Day 3 upload check.

## 5. Named Connection & First Query

### 5.1 Create the `training` connection

Run in your terminal:

```text
snow connection add -n training
```

Follow the prompts using your account identifier, username, class role, and
warehouse. Database and schema can remain unset until the schema setup runs.
If `training` already exists, test it before changing its configuration.

For password authentication, leave the password out of the saved file and
supply it through an environment variable when you connect. For configured
browser SSO, select `externalbrowser` as the authenticator and leave the
password empty. SSO must be available for your account; it is not assumed
for individual trial accounts.

### 5.2 Where settings and credentials are stored

The wizard writes connection settings to a local TOML configuration file.
Find your installation's configuration location with:

```text
snow --info
```

If `~/.snowflake` exists, the default file is `~/.snowflake/config.toml`.
Otherwise the operating-system defaults are:

| Platform | Default file |
|----------|--------------|
| Windows | `%USERPROFILE%\AppData\Local\snowflake\config.toml` |
| macOS | `~/Library/Application Support/snowflake/config.toml` |
| Linux | `~/.config/snowflake/config.toml` |

Explicit configuration options and `SNOWFLAKE_HOME` can override the path.
Here is an illustrative password-authentication profile with the password omitted:

```toml
[connections.training]
account = "myorg-myaccount"
user = "myuser"
role = "SYSADMIN"
warehouse = "COMPUTE_WH"
```

Replace the example values with your own. A `password = "..."` entry, if
saved, is **plain text**. Do not share credentials in screenshots or lab files.
On macOS/Linux, restrict the configuration file to owner read/write permissions.

If `connections.toml` exists in the same directory, CLI reads connections from
that file instead. Its section is `[training]`, without the `connections.`
prefix. Check which file is active before editing settings.

For password authentication, PowerShell can prompt without showing the secret
in the command or terminal history:

```powershell
$trainingCredential = Get-Credential -UserName 'YOUR_USERNAME' -Message 'Snowflake password'
$env:SNOWFLAKE_CONNECTIONS_TRAINING_PASSWORD = $trainingCredential.GetNetworkCredential().Password
```

Replace `YOUR_USERNAME` first. The environment variable contains the password
for this terminal session; it is not an encrypted credential store. Clear it
when finished, or close the terminal:

```powershell
Remove-Item Env:SNOWFLAKE_CONNECTIONS_TRAINING_PASSWORD -ErrorAction SilentlyContinue
Remove-Variable trainingCredential -ErrorAction SilentlyContinue
```

For macOS/Linux Bash, an equivalent hidden prompt is:

```bash
read -r -s -p "Snowflake password: " SNOWFLAKE_CONNECTIONS_TRAINING_PASSWORD
printf '\n'
export SNOWFLAKE_CONNECTIONS_TRAINING_PASSWORD
```

Use `unset SNOWFLAKE_CONNECTIONS_TRAINING_PASSWORD` when finished. These are
Bash commands; macOS users can start `bash` first. SSO users skip password
variable setup.

### 5.3 Test the connection

With your password variable set, or SSO configured, run:

```text
snow connection test -c training
snow sql -c training -q "SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()"
```

Expected: connection status `OK`, followed by a row showing the intended
account, user, role, and warehouse. Approve MFA if prompted. Each CLI invocation
can open a new session, so do not assume session context persists between commands.

### 5.4 File execution and the Day 3 upload check

Run SQL files from the terminal using a quoted path:

```text
snow sql -c training -f "path/to/your-script.sql"
```

The path is on the computer where CLI runs. On Day 3, follow Demo 3 in
`13-InternalStagesLab.sql` to upload the supplied `movies.csv`. Verify the
`PUT` result and `LIST` output; connection status `OK` alone does not prove
access to stage-storage endpoints.

## 6. Lab Files Setup

### 6.1 Obtain a local copy

Download and extract the instructor's lab package to a writable local folder:

```text
Labs/
  SetUp/       Setup scripts and this guide
  Day1/        Introduction and data loading
  Day2/        Table types, cloning, Time Travel, SQL, and scripting
  Day3/        Snowflake CLI, internal stages, Python, and clustering
  Day4/        Streams, tasks, external tables, and other topics

```

A Git-backed Snowsight workspace does not automatically put `movies.csv` on
your laptop or VM. Download the Day3 files onto the machine running CLI.
Paths can contain spaces; quote file paths as shown in the internal-stages lab.

### 6.2 Schema setup

Before Day 1, open a SQL file in Snowsight Workspaces and run
`SetUp/SCHEMA-SETUP-SCOTT.sql` using the instructor-approved role. Verify that
`DEMO_DB.SCOTT.DEPT` and `DEMO_DB.SCOTT.EMP` contain data. The CLI connection
itself does not create these objects.

### 6.3 Git setup for this class

For this class, run `SetUp/GitSetUp.sql` with the
required administrative privileges, then follow that file's workspace setup
instructions. Complete Git setup before opening the schema setup script from the repository. Keep a local copy of files used for CLI transfers.

## 7. Pre-Installed VM Option

IT should provide a supported operating system, a current browser, a writable
student profile, a terminal, and access to the endpoints in Section 4. Install
Snowflake CLI using the official platform-specific procedure and verify
`snow --version` under the student's login. A separate Python installation is
needed for the Python connector demos, with a runtime supported by the chosen
connector release.

Use a tested CLI release consistently across class VMs. For automated
provisioning, follow the current installer or package-manager documentation;
SnowSQL installer switches and paths do not apply to Snowflake CLI.

Pre-install `snowflake-connector-python` in a separate Python virtual environment
for `Day3/demo01.py` and `Day3/demo02.py`. Check the current connector requirements
before selecting Python. Do not bake personal passwords or authentication
caches into the VM image. Students create their own `training` connection.

## 8. Verification Checklist

- [ ] Snowsight login works, including MFA if required.
- [ ] Account identifier and username are known.
- [ ] `snow --version` and `snow --help` work in a new terminal.
- [ ] `snow connection test -c training` reports `OK`.
- [ ] The test query reports the expected user, role, and warehouse.
- [ ] The location of the TOML configuration is understood.
- [ ] IT has approved the required service and stage-storage endpoints.
- [ ] Lab files, including Day3's `movies.csv`, exist locally and are readable.
- [ ] `DEMO_DB.SCOTT.EMP` and `DEMO_DB.SCOTT.DEPT` exist and contain data.
- [ ] If taking the Python demos, the supported Python/connector environment is ready.

## 9. Troubleshooting

| Symptom | Check or action |
|---------|-----------------|
| `snow` is not recognized | Open a new terminal. Verify installation and PATH with IT. |
| Named connection not found | Run `snow --info`; check the active config file, connection name, and whether `connections.toml` takes precedence. |
| Authentication fails | Verify account, username, authenticator, and any required MFA. Password variables apply only to the terminal where they were set. |
| Browser login fails | Confirm SSO is configured and the terminal can launch/access a browser. |
| Connection timeout | Check account endpoints, VPN, proxy, and network policy with IT. |
| Certificate error | Have IT inspect trust/certificate-validation connectivity; do not bypass validation. |
| Warehouse access fails | Confirm the role has USAGE on the selected warehouse. |
| `PUT` fails in Snowsight | Execute the local SQL file through `snow sql -c training -f ...`. |
| Local file not found | Use the absolute path on the CLI machine; retain quotes and use forward slashes in the file URI. |
| Stage not found | Use the lab's database/schema context in the same invocation or fully qualified stage names. |
| SQL works but upload fails | Check local file permissions and the account-specific stage-storage endpoints. |

## References

- [Snowflake CLI installation](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation)
- [CLI configuration](https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-cli)
- [Named connections and authentication](https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections)
- [Executing SQL](https://docs.snowflake.com/en/developer-guide/snowflake-cli/sql/execute-sql)
- [PUT file paths](https://docs.snowflake.com/en/sql-reference/sql/put)
- [Account network allowlist](https://docs.snowflake.com/en/sql-reference/functions/system_allowlist)
- [Python connector requirements](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-install)

_End of Student Setup Guide_

View this Markdown guide on GitHub using its formatted preview, or open it in an editor with Markdown preview.

Python demos prompt for your account, username, and password. Complete authentication as required by your account; these demos must be validated with the class account before use.

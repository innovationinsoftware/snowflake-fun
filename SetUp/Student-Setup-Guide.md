# Snowflake Fundamentals - Student Setup Guide

## Pre-Class IT Preparation

**Copyright © 2026 Innovation In Software Corporation. All rights reserved.**

### 1. Purpose and responsibilities

This guide is for IT administrators preparing student computers or VMs before class. Install Snowflake CLI with the required administrative privileges, then verify that it runs under the student's normal user account without elevation. Students may not have permission to install software themselves.

Windows is the preferred operating system for class demonstrations. Students may use macOS; see Section 3. For other operating systems, follow the official installation documentation and coordinate with the instructor before class.

Students will create their Snowflake trial accounts, configure authentication and a named connection, and test SQL and file transfers during class. No Snowflake account, password, connection profile, schema setup, or Git cloning is required for the pre-class checks below.

### 2. Windows installation and verification

Download **Snowflake CLI** from the [official download page](https://www.snowflake.com/en/developers/downloads/snowflake-cli/). Select the Windows **MSI installer** appropriate for the computer and run it with the necessary permissions. SnowSQL is a different, legacy tool and is not required.

After installation, sign in as the student and open a **new Command Prompt window**. Run:

```text
snow --version
```

On the first run, `Unpacking distribution (tar.bz2)` may appear. Wait for preparation to finish and the version number to display. Do not interrupt it. Verify this works under the student's account, not only under the administrator's account.

If `snow` is not recognized, open a new Command Prompt and have IT check the installation and the student's PATH. Resolve application-control or write-permission restrictions before class. Routine CLI use must not require administrator access.

### 3. macOS and other operating systems

For macOS, use the macOS package installer linked from the [official Snowflake CLI installation guide](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation), selecting the package for the Mac's architecture. Have IT complete any installation approvals. Open a new Terminal window under the student's account and run `snow --version`.

The official guide also documents Homebrew and other supported platforms, including Linux. Windows commands such as `notepad`, `%LOCALAPPDATA%`, and `chcp` do not apply to macOS. The account-free CLI checks `snow --version`, `snow --info`, and `snow helpers detect-encoding` can be run in Terminal.

<!-- PAGE -->

### 4. Configuration and optional encoding check

Under the student's account, run:

```text
snow --info
```

Find **`default_config_file_path`**. Other JSON fields do not need to be interpreted for this preparation. `SNOWFLAKE_HOME: null` by itself is not an error. Configuration is user-specific; do not prepare only the administrator's profile.

On Windows, if the displayed path is in `%LOCALAPPDATA%\snowflake`, open the configuration with:

```text
notepad "%LOCALAPPDATA%\snowflake\config.toml"
```

Windows substitutes the current user's folder automatically. If `snow --info` shows another path, use that exact path instead. On macOS, use a plain-text editor and the path reported by `snow --info`.

No connection settings are needed yet. If the configuration file does not exist, create it only if the encoding correction below is needed. Use the reported location, create its parent folder if necessary, and save as `config.toml`, not `config.toml.txt`. Ensure the student can access it. On macOS, restrict the file to owner read/write permissions as described in the [configuration documentation](https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-cli).

**Optional troubleshooting: Windows encoding warning**

Check the environment in Command Prompt:

```text
snow helpers detect-encoding
```

If an encoding warning appears, add the following section to `config.toml`. Preserve all other settings. If `[cli.encoding]` exists, update it instead of creating a duplicate.

```toml
[cli.encoding]
file_io = "utf-8"
subprocess = "utf-8"
stdout = "utf-8"
```

Save the file. If the diagnostic separately reports that the console is not using UTF-8, run the following in that console. It applies only to the current console session:

```text
chcp 65001
```

Repeat `snow helpers detect-encoding`. Expected result:

```text
No encoding issues - your system is properly configured.
```

This correction was verified on the instructor's Windows computer with Snowflake CLI 3.27.0. Changing `PYTHONUTF8` did not resolve the warning in that test; the `config.toml` settings did. Use this procedure rather than adding alternative fixes.

<!-- PAGE -->

### 5. Network preparation

For corporate computers and VMs, arrange access before class to the Snowflake CLI download source, Snowflake documentation, the [Snowflake signup page](https://signup.snowflake.com/), [Snowsight](https://app.snowflake.com/), and the [class Git repository](https://github.com/innovationinsoftware/snowflake-fun). Verify that these pages open from the student's browser on the network or VPN that will be used in class. Permit required download redirects through your organization's approved process.

Snowflake use also requires account-service, stage-storage, and certificate-validation endpoints. Most traffic uses outbound HTTPS on port 443; some certificate-validation endpoints require port 80. Normal client use does not require an inbound listener. Identity-provider endpoints may be needed if the account uses SSO.

**The account-specific endpoint list is not available until the trial account is created in class.** Arrange for IT assistance during the first session if the network uses a strict allowlist. After account creation, the instructor can obtain the account-specific list using `SYSTEM$ALLOWLIST()` and provide it to IT. See the [official allowlist reference](https://docs.snowflake.com/en/sql-reference/functions/system_allowlist).

Opening a webpage verifies only that page's accessibility. It does not prove Snowflake login, SQL execution, or file-transfer connectivity. Those checks will be performed in class after account creation.

If a proxy is required, IT should provide and validate its settings for both the browser and CLI under the student's account. For Windows Command Prompt, these are placeholder examples, not values to use unchanged:

```text
set HTTPS_PROXY=http://proxy.yourcompany.com:8080
set HTTP_PROXY=http://proxy.yourcompany.com:8080
```

These commands affect the current Command Prompt session. On macOS, IT can configure the corresponding `HTTPS_PROXY` and `HTTP_PROXY` environment variables in the student's shell. IT should determine any `NO_PROXY` exclusions. Do not embed proxy passwords in shared instructions. For certificate errors, check the trust chain, proxy, endpoints, and system clock; do not disable certificate validation.

### 6. Final readiness checklist

- [ ] Snowflake CLI is installed on the computer or VM the student will use.
- [ ] `snow --version` succeeds under the student's normal account without elevation.
- [ ] Initial unpacking has completed; a new terminal can run the CLI.
- [ ] `snow --info` runs, and the student's configuration location is known.
- [ ] Any encoding warning has been addressed using Section 4.
- [ ] A current browser, a PDF reader/browser preview, and a writable local lab folder are available.
- [ ] Required pre-class websites and downloads are accessible from the class network/VPN.
- [ ] Proxy requirements are documented and an IT contact is available for account-specific network issues during class.

Record the operating system, installed CLI version, and IT contact for the instructor. The computer is ready when these local checks pass; creating an account and testing a Snowflake connection are class activities.

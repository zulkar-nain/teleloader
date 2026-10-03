# TeleLoader

A small asynchronous Telegram video downloader built with Telethon. It scans a chat's message history, queues video documents, and downloads them with a configurable number of concurrent workers. Rich displays transfer progress in the terminal.

Use this only for chats and media you are authorized to access and save. The script does not bypass Telegram's download restrictions; if saving is disabled for a chat, ask an administrator to enable it or provide the media through an approved channel.

## Requirements

- Windows, macOS, or Linux
- Python 3.10 or newer
- A Telegram account
- Telegram API credentials (`API_ID` and `API_HASH`) from [my.telegram.org](https://my.telegram.org)

## Set up

Open a terminal in the project folder and create a virtual environment if you have not already:

```powershell
python -m venv .venv
```

Activate it in PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

On macOS or Linux, activate it with:

```sh
source .venv/bin/activate
```

Install the libraries into the active environment:

```sh
python -m pip install --upgrade pip
python -m pip install telethon rich cryptg
```

`cryptg` is an optional acceleration library for Telethon. If it cannot be installed for your Python version or platform, install the required packages without it:

```sh
python -m pip install telethon rich
```

## Configure

Edit the configuration constants near the top of [`async_downloader.py`](./%28async_downloader.py):

- `API_ID`: Your numeric API ID from my.telegram.org.
- `API_HASH`: Your API hash from my.telegram.org. Keep it private.
- `CHAT_TARGET`: The chat's numeric ID or a username Telethon can resolve, such as `"public_group"`.
- `OUTPUT_DIR`: Destination directory for downloaded files. Defaults to `./downloads`.
- `MAX_CONCURRENT_DOWNLOADS`: Number of asynchronous workers. Start with `1` or `2`; increasing concurrency can cause Telegram to apply FloodWait delays.

The first run may ask for your phone number, the login code Telegram sends you, and your two-step verification password if enabled. Telethon stores the authorized session locally as `session_downloader.session`. Treat this file like a password: anyone who obtains it may be able to access your Telegram account. Do not share it or commit it.

## Run

With the virtual environment activated, run:

```powershell
python '.\async_downloader.py'
```

Or run it without activating the environment:

```powershell
.\.venv\Scripts\python.exe '.\async_downloader.py'
```

The script scans the chat from older messages to newer messages, downloads messages containing video documents, and saves each file under `downloads/` with its Telegram message ID prefixed to the filename. Files already present at the expected path are skipped. FloodWait errors pause the affected worker for the duration Telegram specifies, plus a short random delay.

Press `Ctrl+C` to stop the process. A later run can download files that are still missing.

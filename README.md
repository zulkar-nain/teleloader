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
python -m pip install telethon aiohttp cryptg
```

`cryptg` is an optional acceleration library for Telethon. If it cannot be installed for your Python version or platform, install the required packages without it:

```sh
python -m pip install telethon aiohttp
```

## Configure

Provide your credentials from my.telegram.org as environment variables (never hardcode them). In PowerShell, for the current session:

```powershell
$env:TELEGRAM_API_ID = "12345678"
$env:TELEGRAM_API_HASH = "your_api_hash"
```

On macOS or Linux, use `export TELEGRAM_API_ID=...` and `export TELEGRAM_API_HASH=...`.

The first run asks in the terminal for your phone number, the login code Telegram sends you, and your two-step verification password if enabled. Telethon stores the authorized session locally as `session_downloader.session`. Treat this file like a password: anyone who obtains it may be able to access your Telegram account. Do not share it or commit it.

The chat, download folder, media types (videos, photos, audio, other files), and number of concurrent downloads are chosen in the web UI, not in code.

## Run

With the virtual environment activated and the variables set:

```powershell
python .\async_downloader.py
```

Open http://127.0.0.1:8080. Pick a chat from the dropdown, choose a download folder (type a path or use Browse), set the concurrent downloads (1-8; start with 2-3, since higher values can cause FloodWait delays), then press Start. Stop cancels the run and removes any partially downloaded file.

The script scans the chat from older messages to newer messages, downloads messages containing the selected media types, and saves each file with its Telegram message ID prefixed to the filename. Files already present at the expected path are skipped. FloodWait errors pause the affected worker for the duration Telegram specifies, plus a short random delay.

Press `Ctrl+C` in the terminal to quit. A later run can download files that are still missing.

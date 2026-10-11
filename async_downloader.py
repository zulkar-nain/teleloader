import os
import json
import time
import asyncio
import random
from collections import deque
from pathlib import Path
from aiohttp import web
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto

UI_HOST = "127.0.0.1"  # Keep local: this process holds your Telegram session
UI_PORT = 8080
WEB_DIR = Path(__file__).parent / "web"
DEFAULT_OUTPUT_DIR = "./downloads"
DEFAULT_WORKERS = 3
MAX_WORKERS = 8  # Higher values quickly trigger FloodWait
MEDIA_KINDS = ("video", "photo", "audio", "file")
DEFAULT_MEDIA = ["video"]


def media_kind(message):
    media = message.media
    if isinstance(media, MessageMediaPhoto):
        return "photo"
    if isinstance(media, MessageMediaDocument) and media.document:
        mime = getattr(media.document, "mime_type", "") or ""
        for prefix, kind in (("video/", "video"), ("image/", "photo"), ("audio/", "audio")):
            if mime.startswith(prefix):
                return kind
        return "file"
    return None


def load_credentials():
    try:
        return int(os.environ["TELEGRAM_API_ID"]), os.environ["TELEGRAM_API_HASH"]
    except (KeyError, ValueError):
        raise SystemExit(
            "Set the TELEGRAM_API_ID (number) and TELEGRAM_API_HASH environment variables."
        )


class State:
    """Shared download state, serialized to the browser UI."""

    def __init__(self):
        self.config = {
            "chat_id": None,
            "chat_name": "",
            "output_dir": DEFAULT_OUTPUT_DIR,
            "workers": DEFAULT_WORKERS,
            "media_types": DEFAULT_MEDIA,
        }
        self.logs = deque(maxlen=100)
        self.reset()

    def reset(self):
        self.status = "idle"  # idle | scanning | downloading | paused | done | stopped | error
        self.total_files = 0
        self.total_bytes = 0
        self.completed = 0
        self.skipped = 0
        self.failed = 0
        self.failures = []  # {id, name, size, error, t}
        self.finished_bytes = 0
        self.active = {}  # msg_id -> {id, name, worker, current, total}
        self.started_at: float | None = None
        self.finished_at: float | None = None

    def log(self, level: str, text: str):
        self.logs.append({"t": time.strftime("%H:%M:%S"), "level": level, "text": text})

    def add_failure(self, msg_id: int, name: str, size: int, err: Exception):
        self.failed += 1
        self.failures.append({
            "id": msg_id,
            "name": name,
            "size": size,
            "error": f"{type(err).__name__}: {err}",
            "t": time.strftime("%H:%M:%S"),
        })
        self.log("error", f"Failed msg {msg_id}: {err}")

    def snapshot(self) -> dict:
        in_progress = sum(a["current"] for a in self.active.values())
        end = self.finished_at or time.time()
        return {
            "status": self.status,
            "config": self.config,
            "total_files": self.total_files,
            "total_bytes": self.total_bytes,
            "completed": self.completed,
            "skipped": self.skipped,
            "failed": self.failed,
            "failures": self.failures,
            "downloaded_bytes": self.finished_bytes + in_progress,
            "elapsed": (end - self.started_at) if self.started_at else 0,
            "active": list(self.active.values()),
            "logs": list(self.logs),
        }


class TelegramBatchDownloader:
    def __init__(self, client: TelegramClient, output_dir: str, state: State, max_workers: int, media_types):
        self.client = client
        self.output_dir = output_dir
        self.state = state
        self.max_workers = max_workers
        self.media_types = set(media_types)
        self.queue = asyncio.Queue()
        self.resume = asyncio.Event()  # cleared while paused
        self.resume.set()

    async def worker(self, worker_id: int):
        """Worker task to consume and process messages from the queue."""
        state = self.state
        while True:
            await self.resume.wait()
            message = await self.queue.get()
            if message is None:  # Sentinel to shut down worker
                self.queue.task_done()
                break

            msg_id = message.id
            size = message.file.size or 0
            kind = media_kind(message) or "file"
            file_name = f"{msg_id}_{message.file.name or kind + (message.file.ext or '')}"
            file_path = os.path.join(self.output_dir, file_name)

            if os.path.exists(file_path):
                state.skipped += 1
                state.finished_bytes += size
                state.log("warn", f"Skipped msg {msg_id}: already downloaded")
                self.queue.task_done()
                continue

            entry = {"id": msg_id, "name": file_name, "worker": worker_id, "current": 0, "total": size}
            state.active[msg_id] = entry

            async def progress_callback(current: int, total: int):
                await self.resume.wait()  # holds the transfer while paused
                entry["current"] = current
                entry["total"] = total or entry["total"]

            download_success = False
            while not download_success:
                try:
                    await self.client.download_media(
                        message,
                        file=file_path,
                        progress_callback=progress_callback,  # type: ignore  # Telethon awaits async callbacks; stubs say sync only
                    )
                    download_success = True
                    state.completed += 1
                    state.finished_bytes += size
                    state.log("ok", f"Finished msg {msg_id} -> {file_name}")
                except FloodWaitError as e:
                    wait_time = e.seconds + random.uniform(2, 5)
                    state.log("warn", f"FloodWait on msg {msg_id}: worker {worker_id} paused {wait_time:.0f}s")
                    entry["current"] = 0
                    await asyncio.sleep(wait_time)
                except RPCError as e:
                    state.add_failure(msg_id, file_name, size, e)
                    break
                except Exception as e:
                    state.add_failure(msg_id, file_name, size, e)
                    break
                finally:
                    if not download_success and os.path.exists(file_path):
                        # Also runs on cancellation, so Stop never leaves partial files
                        try:
                            os.remove(file_path)
                        except OSError:
                            pass

            state.active.pop(msg_id, None)
            self.queue.task_done()
            # Inter-request delay to reduce likelihood of triggering FloodWait
            await asyncio.sleep(random.uniform(1.0, 2.5))

    async def run(self, chat_id):
        state = self.state
        state.status = "scanning"
        state.started_at = time.time()
        state.log("info", f"Scanning '{state.config['chat_name']}' for: {', '.join(sorted(self.media_types))}")

        async for message in self.client.iter_messages(chat_id, reverse=True):
            if message.file and media_kind(message) in self.media_types:
                state.total_files += 1
                state.total_bytes += message.file.size or 0
                await self.queue.put(message)

        if state.total_files == 0:
            state.status = "done"
            state.finished_at = time.time()
            state.log("info", "No matching files found.")
            return

        state.status = "downloading"
        state.log("info", f"Queued {state.total_files} files; starting {self.max_workers} workers")

        workers = [asyncio.create_task(self.worker(i + 1)) for i in range(self.max_workers)]
        try:
            await self.queue.join()

            # Push sentinels to terminate workers
            for _ in range(self.max_workers):
                await self.queue.put(None)
            await asyncio.gather(*workers)
        finally:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        state.status = "done"
        state.finished_at = time.time()
        state.log("info", "All downloads finished.")


class Controller:
    """Starts and stops download runs on behalf of the UI."""

    def __init__(self, client: TelegramClient, state: State):
        self.client = client
        self.state = state
        self.task = None
        self.downloader = None

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def start(self, chat_id: int, chat_name: str, output_dir: str, workers: int, media_types: list):
        self.state.reset()
        self.state.config = {
            "chat_id": chat_id,
            "chat_name": chat_name,
            "output_dir": output_dir,
            "workers": workers,
            "media_types": media_types,
        }
        self.task = asyncio.create_task(self._run(chat_id, output_dir, workers, media_types))

    async def _run(self, chat_id: int, output_dir: str, workers: int, media_types: list):
        state = self.state
        try:
            downloader = TelegramBatchDownloader(self.client, output_dir, state, workers, media_types)
            self.downloader = downloader
            await downloader.run(chat_id)
        except asyncio.CancelledError:
            state.status = "stopped"
            state.log("warn", "Stopped by user")
        except Exception as e:
            state.status = "error"
            state.log("error", f"Run failed: {e}")
        finally:
            state.active.clear()
            state.finished_at = time.time()

    def pause(self):
        if self.downloader and self.running and self.state.status == "downloading":
            self.downloader.resume.clear()
            self.state.status = "paused"
            self.state.log("info", "Paused")

    def resume(self):
        if self.downloader and self.running and self.state.status == "paused":
            self.downloader.resume.set()
            self.state.status = "downloading"
            self.state.log("info", "Resumed")

    async def stop(self):
        if self.task and self.running:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)


def pick_folder(initial: str) -> str:
    import tkinter as tk
    from tkinter import filedialog

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    try:
        return filedialog.askdirectory(initialdir=initial or None, title="Select download folder")
    finally:
        root.destroy()


def error(message: str, status: int = 400) -> web.Response:
    return web.json_response({"error": message}, status=status)


@web.middleware
async def local_only(request: web.Request, handler):
    # Blocks DNS-rebinding and cross-site POSTs aimed at the local API
    hosts = {f"{UI_HOST}:{UI_PORT}", f"localhost:{UI_PORT}"}
    if request.host not in hosts:
        raise web.HTTPForbidden()
    if request.method == "POST" and request.headers.get("Origin") not in {f"http://{h}" for h in hosts}:
        raise web.HTTPForbidden()
    return await handler(request)


def build_app(ctl: Controller) -> web.Application:
    state = ctl.state

    async def index(_request):
        return web.FileResponse(WEB_DIR / "index.html")

    async def events(request):
        resp = web.StreamResponse(headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
        })
        await resp.prepare(request)
        try:
            while True:
                await resp.write(f"data: {json.dumps(state.snapshot())}\n\n".encode())
                await asyncio.sleep(0.5)
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        return resp

    async def chats(_request):
        items = []
        async for d in ctl.client.iter_dialogs():
            kind = "group" if d.is_group else "channel" if d.is_channel else "user"
            items.append({"id": d.id, "name": d.name or str(d.id), "type": kind})
        return web.json_response(items)

    async def start(request):
        if ctl.running:
            return error("A download is already running.", 409)
        try:
            body = await request.json()
            chat_id = int(body["chat_id"])
            workers = max(1, min(MAX_WORKERS, int(body["workers"])))
            output = str(body["output_dir"]).strip()
            media_types = [k for k in MEDIA_KINDS if k in body["media_types"]]
        except (KeyError, ValueError, TypeError):
            return error("Invalid request.")
        if not media_types:
            return error("Select at least one media type.")
        if not output:
            return error("Choose a download folder.")
        try:
            out = Path(output).expanduser().resolve()
            out.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            return error(f"Cannot use that folder: {e}")
        ctl.start(chat_id, str(body.get("chat_name", ""))[:200], str(out), workers, media_types)
        return web.json_response({"ok": True})

    async def stop(_request):
        await ctl.stop()
        return web.json_response({"ok": True})

    async def pause(_request):
        ctl.pause()
        return web.json_response({"ok": True})

    async def resume(_request):
        ctl.resume()
        return web.json_response({"ok": True})

    async def browse(request):
        try:
            body = await request.json()
            initial = str(body.get("initial", ""))
        except (ValueError, TypeError):
            initial = ""
        try:
            path = await asyncio.get_running_loop().run_in_executor(None, pick_folder, initial)
        except Exception as e:
            return error(f"Folder dialog unavailable: {e}", 500)
        return web.json_response({"path": os.path.normpath(path) if path else ""})

    app = web.Application(middlewares=[local_only])
    app.router.add_get("/", index)
    app.router.add_get("/api/events", events)
    app.router.add_get("/api/chats", chats)
    app.router.add_post("/api/start", start)
    app.router.add_post("/api/stop", stop)
    app.router.add_post("/api/pause", pause)
    app.router.add_post("/api/resume", resume)
    app.router.add_post("/api/browse", browse)
    app.router.add_static("/static/", WEB_DIR)
    return app


async def main():
    api_id, api_hash = load_credentials()

    # Log in first so any phone/code prompts appear before the UI starts
    async with TelegramClient("session_downloader", api_id, api_hash) as client:
        ctl = Controller(client, State())
        runner = web.AppRunner(build_app(ctl))
        await runner.setup()
        await web.TCPSite(runner, UI_HOST, UI_PORT).start()
        print(f"UI running at http://{UI_HOST}:{UI_PORT}  (Ctrl+C to quit)")

        try:
            await asyncio.Event().wait()
        finally:
            await ctl.stop()
            await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

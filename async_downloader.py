import os
import asyncio
import random
from typing import Set
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import MessageMediaDocument
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
    TaskID,
)

# Configuration
API_ID = 111 # Replace with your API_ID (int)
API_HASH = "xxx"  # Replace with your API_HASH (str)
CHAT_TARGET = 111  # Target Chat ID or channel username
OUTPUT_DIR = "./downloads"
MAX_CONCURRENT_DOWNLOADS = 3  # Keep between 2-4 to avoid quick FloodWait bans


class TelegramBatchDownloader:
    def __init__(self, client: TelegramClient, output_dir: str, max_workers: int = 3):
        self.client = client
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.queue = asyncio.Queue()
        self.downloaded_ids: Set[int] = set()
        os.makedirs(self.output_dir, exist_ok=True)

    async def worker(self, worker_id: int, progress: Progress):
        """Worker task to consume and process messages from the queue."""
        while True:
            message = await self.queue.get()
            if message is None:  # Sentinel to shut down worker
                self.queue.task_done()
                break

            msg_id = message.id
            file_name = f"{msg_id}_{message.file.name or 'video.mp4'}"
            file_path = os.path.join(self.output_dir, file_name)

            if os.path.exists(file_path):
                progress.console.print(f"[yellow]Skipping Msg {msg_id}: Already downloaded.[/yellow]")
                self.queue.task_done()
                continue

            # Add task to progress bar interface
            task_id: TaskID = progress.add_task(
                f"[cyan]Worker {worker_id} - Msg {msg_id}",
                total=message.file.size or 0,
            )

            def progress_callback(current: int, total: int):
                progress.update(task_id, completed=current, total=total)

            download_success = False
            while not download_success:
                try:
                    await self.client.download_media(
                        message,
                        file=file_path,
                        progress_callback=progress_callback,
                    )
                    download_success = True
                    progress.console.print(f"[green]✓ Finished Msg {msg_id} -> {file_name}[/green]")
                except FloodWaitError as e:
                    wait_time = e.seconds + random.uniform(2, 5)
                    progress.console.print(
                        f"[bold red]![/bold red] FloodWait on Msg {msg_id}. Pausing Worker {worker_id} for {wait_time:.1f}s..."
                    )
                    await asyncio.sleep(wait_time)
                except RPCError as e:
                    progress.console.print(f"[red]RPC Error on Msg {msg_id}: {e}[/red]")
                    break
                except Exception as e:
                    progress.console.print(f"[red]Unexpected error on Msg {msg_id}: {e}[/red]")
                    break
                finally:
                    if not download_success and os.path.exists(file_path):
                        # Clean up incomplete chunk files
                        try:
                            os.remove(file_path)
                        except OSError:
                            pass

            progress.remove_task(task_id)
            self.queue.task_done()
            # Inter-request delay to reduce likelihood of triggering FloodWait
            await asyncio.sleep(random.uniform(1.0, 2.5))

    async def run(self, chat_id):
        # Scan and queue relevant video messages
        print(f"Scanning target chat ({chat_id}) for video media...")
        async for message in self.client.iter_messages(chat_id, reverse=True):
            if message.media and isinstance(message.media, MessageMediaDocument):
                mime = message.media.document.mime_type or ""
                if mime.startswith("video/"):
                    await self.queue.put(message)

        total_files = self.queue.qsize()
        if total_files == 0:
            print("No video files found.")
            return

        print(f"Queued {total_files} video files. Initializing {self.max_workers} async workers...")

        # Setup Rich progress UI
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            # Spawn worker tasks
            workers = [
                asyncio.create_task(self.worker(i + 1, progress))
                for i in range(self.max_workers)
            ]

            # Wait for all queue items to process
            await self.queue.join()

            # Push sentinels to terminate workers
            for _ in range(self.max_workers):
                await self.queue.put(None)

            await asyncio.gather(*workers)


async def main():
    async with TelegramClient("session_downloader", API_ID, API_HASH) as client:
        downloader = TelegramBatchDownloader(
            client=client,
            output_dir=OUTPUT_DIR,
            max_workers=MAX_CONCURRENT_DOWNLOADS,
        )
        await downloader.run(CHAT_TARGET)


if __name__ == "__main__":
    asyncio.run(main())
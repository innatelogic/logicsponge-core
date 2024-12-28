import csv
import logging
import os
import tempfile
import time

import chardet
import gdown
import watchdog
import watchdog.events
import watchdog.observers
from watchdog.observers.api import BaseObserver

import logicsponge.core as ls

# logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    encoding="utf-8",
    format="%(message)s",
    level=logging.INFO,
)


class FileWatchHandler(watchdog.events.FileSystemEventHandler):
    file_path: str
    encoding: str
    source: "FileWatchSource"

    def __init__(self, file_path: str, source: "FileWatchSource", encoding: str = "utf-8") -> None:
        super().__init__()
        self.file_path = os.path.abspath(file_path)
        self.source = source
        self.encoding = encoding

    def read_file(self) -> None:
        # file was changed
        with open(self.file_path, encoding=self.encoding) as file:
            data = file.read()
            timestamp = os.path.getmtime(self.file_path)
            self.source.output(ls.DataItem({"Time": timestamp, "string": data}))

    def on_modified(self, event: watchdog.events.FileSystemEvent) -> None:
        if event.src_path == self.file_path:
            self.read_file()


class FileWatchSource(ls.SourceTerm):
    observer: BaseObserver
    handler: FileWatchHandler

    def __init__(self, file_path: str, *args, encoding: str = "utf-8", **kwargs):
        super().__init__(*args, **kwargs)

        # setup the file watcher
        self.handler = FileWatchHandler(file_path=file_path, source=self, encoding=encoding)

        # observe the directory of the file
        self.observer = watchdog.observers.Observer()
        path = os.path.dirname(file_path)
        self.observer.schedule(self.handler, path=path, recursive=False)

    def enter(self):
        self.handler.read_file()
        self.observer.start()

    def exit(self):
        self.observer.stop()
        self.observer.join()

    def run(self):
        while True:
            time.sleep(60)  # TODO: better way?


class CSVStreamer(ls.SourceTerm):
    def __init__(self, *args, file_path: str, delay: float = 0, poll_delay: float = 1, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.delay = delay
        self.poll_delay = poll_delay
        self.position = 0

    def run(self):
        while True:
            try:
                with open(self.file_path) as csvfile:
                    # Move the pointer to the last read position
                    csvfile.seek(self.position)

                    # Read new lines if available
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        time.sleep(self.delay)
                        out = ls.DataItem(row)
                        self.output(out)

                    self.position = csvfile.tell()

                time.sleep(self.poll_delay)
            except Exception as e:
                msg = f"Error while streaming CSV: {e}"
                logger.exception(msg)
                break


class GoogleDriveSource(ls.SourceTerm):
    poll_interval_sec: int
    google_drive_link: str | None
    local_filename: str

    def __init__(self, google_drive_link: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.google_drive_link = google_drive_link
        self.poll_interval_sec = kwargs.get("poll_interval_sec", 60)
        self.local_filename = os.path.join(tempfile.mkdtemp(), "file.txt")

    def download(self) -> None:
        gdown.download(url=self.google_drive_link, output=self.local_filename, fuzzy=True, quiet=True)

    def run(self) -> None:
        while True:
            if self.google_drive_link is None:
                return

            try:
                self.download()

                encoding = None
                with open(self.local_filename, "rb") as file:
                    raw_file_contents = file.read()
                    encoding = chardet.detect(raw_file_contents)["encoding"]

                file_contents = ""
                with open(self.local_filename, encoding=encoding) as file:
                    file_contents = file.read()

                self.output(ls.DataItem({"Time": time.time(), "string": file_contents}))

            finally:
                time.sleep(self.poll_interval_sec)


class StringDiff(ls.FunctionTerm):
    old_string: str  # state

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.old_string = ""

    def f(self, data: ls.DataItem) -> ls.DataItem:
        new_string = data["string"]
        ret_string = new_string[len(self.old_string) :] if new_string.startswith(self.old_string) else new_string
        self.old_string = new_string
        return ls.DataItem({**data, "string": ret_string})


class LineSplitter(ls.FunctionTerm):
    def f(self, data: ls.DataItem) -> None:
        lines = data["string"].replace("\r\n", "\n").split("\n")
        for line in lines:
            self.output(ls.DataItem({**data, "string": line}))


class LineParser(ls.FunctionTerm):
    comment: str
    delimiter: str
    has_header: bool
    header: list[str] | None  # state

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.comment = kwargs.get("comment", "#")
        self.delimiter = kwargs.get("delimiter", "\t")
        self.has_header = kwargs.get("has_header", True)
        self.header = None

    def f(self, data: ls.DataItem) -> ls.DataItem | None:
        line = data["string"]
        if len(line) > 0 and line[0] == self.comment:
            # comment line
            return None

        if self.header is None:
            # set header
            if self.has_header:
                self.header = line.split(self.delimiter)
                return None
            line_list = line.split(self.delimiter)
            self.header = [str(i) for i in range(len(line_list))]
            # do not return but continue since no header

        line_list = line.split(self.delimiter)
        if len(line_list) != len(self.header):
            msg = f'line has more cols than header: "{line}"'
            raise ValueError(msg)

        return ls.DataItem({self.header[i]: line_list[i] for i in range(len(line_list))})

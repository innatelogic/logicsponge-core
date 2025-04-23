"""Sources for logicsponge."""

import csv
import logging
import tempfile
import time
from collections.abc import Callable, Hashable, Iterable
from pathlib import Path
from typing import Any

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
    """Watches a file for changes."""

    file_path: Path
    encoding: str
    source: "FileWatchSource"

    def __init__(self, file_path: str, source: "FileWatchSource", encoding: str = "utf-8") -> None:
        """Create a FileWatchHandler object."""
        super().__init__()
        self.file_path = Path(file_path).resolve()
        self.source = source
        self.encoding = encoding

    def read_file(self) -> None:
        """Read the file."""
        # file was changed
        with Path.open(self.file_path, encoding=self.encoding) as file:
            data = file.read()
            timestamp = Path(self.file_path).stat().st_mtime
            self.source.output(ls.DataItem({"Time": timestamp, "string": data}))

    def on_modified(self, event: watchdog.events.FileSystemEvent) -> None:
        """Call if file was modified."""
        if event.src_path == self.file_path:
            self.read_file()


class FileWatchSource(ls.SourceTerm):
    """Source that watches a file for changes."""

    observer: BaseObserver
    handler: FileWatchHandler

    def __init__(self, file_path: str, *args, encoding: str = "utf-8", **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a FileWatchSource object."""
        super().__init__(*args, **kwargs)

        # setup the file watcher
        self.handler = FileWatchHandler(file_path=file_path, source=self, encoding=encoding)

        # observe the directory of the file
        self.observer = watchdog.observers.Observer()
        path = str(Path(file_path).parent)
        self.observer.schedule(self.handler, path=path, recursive=False)

    def enter(self) -> None:
        """Enter the source."""
        self.handler.read_file()
        self.observer.start()

    def exit(self) -> None:
        """Exit the source."""
        self.observer.stop()
        self.observer.join()

    def run(self) -> None:
        """Execute the source's run."""
        while True:
            time.sleep(60)  # TODO: better way?


class CSVStreamer(ls.SourceTerm):
    """Stream a csv file."""

    file_path: str
    poll_delay: float
    position: int

    def __init__(self, *args, file_path: str, poll_delay: float = 1, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a CSVStreamer object."""
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.poll_delay = poll_delay
        self.position = 0

    def run(self) -> None:
        """Execute the run."""
        # TODO: unclear about updates within line. Not explicitely managed.
        while True:
            try:
                with Path(self.file_path).open() as csvfile:
                    # Move the pointer to the last read position
                    csvfile.seek(self.position)

                    # Read new lines if available
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        out = ls.DataItem(row)
                        self.output(out)

                    self.position = csvfile.tell()

                time.sleep(self.poll_delay)
            except Exception as e:
                msg = f"Error while streaming CSV: {e}"
                logger.exception(msg)
                break


class GoogleDriveSource(ls.SourceTerm):
    """Stream a Google drive url."""

    poll_interval_sec: int
    google_drive_link: str | None
    local_filename: Path

    def __init__(self, google_drive_link: str, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a GoogleDriveSource object."""
        super().__init__(*args, **kwargs)

        self.google_drive_link = google_drive_link
        self.poll_interval_sec = kwargs.get("poll_interval_sec", 60)
        self.local_filename = Path(tempfile.mkdtemp()) / "file.txt"

    def download(self) -> None:
        """Download the file."""
        gdown.download(url=self.google_drive_link, output=str(self.local_filename), fuzzy=True, quiet=True)

    def run(self) -> None:
        """Execute the run."""
        while True:
            if self.google_drive_link is None:
                return

            try:
                self.download()

                encoding = None
                with Path.open(self.local_filename, "rb") as file:
                    raw_file_contents = file.read()
                    encoding = chardet.detect(raw_file_contents)["encoding"]

                file_contents = ""
                with Path.open(self.local_filename, encoding=encoding) as file:
                    file_contents = file.read()

                self.output(ls.DataItem({"Time": time.time(), "string": file_contents}))

            finally:
                time.sleep(self.poll_interval_sec)


class StringDiff(ls.FunctionTerm):
    """Compute the diff of strings."""

    old_string: str  # state

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a StringDiff object."""
        super().__init__(*args, **kwargs)
        self.old_string = ""

    def f(self, data: ls.DataItem) -> ls.DataItem:
        """Execute on new data."""
        new_string = data["string"]
        ret_string = new_string.removeprefix(self.old_string)
        self.old_string = new_string
        return ls.DataItem({**data, "string": ret_string})


class LineSplitter(ls.FunctionTerm):
    """Split into lines."""

    def f(self, data: ls.DataItem) -> None:
        """Execute on new data."""
        lines = data["string"].replace("\r\n", "\n").split("\n")
        for line in lines:
            self.output(ls.DataItem({**data, "string": line}))


class LineParser(ls.FunctionTerm):
    """Parse lines."""

    comment: str
    delimiter: str
    has_header: bool
    header: list[str] | None  # state

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a LineParser object."""
        super().__init__(*args, **kwargs)
        self.comment = kwargs.get("comment", "#")
        self.delimiter = kwargs.get("delimiter", "\t")
        self.has_header = kwargs.get("has_header", True)
        self.header = None

    def f(self, data: ls.DataItem) -> ls.DataItem | None:
        """Execute on new data."""
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


class IterableSource(ls.SourceTerm):
    """Stream an Iterable like a list, tuple, etc."""

    _iterable: Iterable
    _formatter: Callable[[Any], dict[str, Hashable]]

    @staticmethod
    def formatter_default(item: Any) -> dict[str, Hashable]:  # noqa: ANN401
        """Format an item per default."""
        if isinstance(item, dict):
            return item
        return {"val": item}

    @staticmethod
    def _is_iterable(item: Any) -> bool:  # noqa: ANN401
        """Return is item is an Iterable, but exclude the non-intended str and byte."""
        return isinstance(item, Iterable) and not isinstance(item, (str, bytes))

    def __init__(
        self,
        iterable: Iterable = (),
        name: str | None = None,
        formatter: Callable[[Any], dict[str, Hashable]] = formatter_default,
        **kwargs,  # noqa: ANN003
    ) -> None:
        """Create an IterableSource.

        Arguments:
            iterable (Iterable, optional): The Iterable, over which the source will iterate and stream
                to its output.
                By default, the items are expected to be dicts with keys as str.
                If not, use the formatter keyword.
            name (str | None, optional): The name of the Term.
            formatter (Callable[[Any], dict[str, Hashable]], optional): A function that is called on each item
                of the iterable and that is expected to return a dict with keys as strings.
                By the fault the identity is used as the formatter.
            **kwargs: Remaining kwargs.

        """
        if not self._is_iterable(iterable):
            msg = f"'{iterable}' is not a valid Iterable."
            raise TypeError(msg)
        self._iterable = iterable
        self._formatter = formatter
        super().__init__(name=name, **kwargs)

    def run(self) -> None:
        """Execute the run."""
        for item in self._iterable:
            formatted = self._formatter(item)
            self.output(ls.DataItem(formatted))

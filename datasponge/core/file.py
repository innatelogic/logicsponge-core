import csv
import logging
import time

import datasponge.core as ds

logger = logging.getLogger(__name__)


class CSVStreamer(ds.SourceTerm):
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
                        out = ds.DataItem(row)
                        self.output(out)

                    self.position = csvfile.tell()

                time.sleep(self.poll_delay)
            except Exception as e:
                msg = f"Error while streaming CSV: {e}"
                logger.exception(msg)
                break

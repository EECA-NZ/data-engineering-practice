"""Controller module to manage file downloading and extraction processes."""
import asyncio
from tenacity import RetryError
from .manager import FileManager
from .logger_view import LoggerView


class FileController:
    """Controller to coordinate file downloading and extraction."""

    FILE_NAMES = [
        "Divvy_Trips_2018_Q4.zip",
        "Divvy_Trips_2019_Q1.zip",
        "Divvy_Trips_2019_Q2.zip",
        "Divvy_Trips_2019_Q3.zip",
        "Divvy_Trips_2019_Q4.zip",
        "Divvy_Trips_2020_Q1.zip",
        "Divvy_Trips_2220_Q1.zip",  # This is an incorrect URL for testing
    ]

    def __init__(self):
        """Initialize the controller with FileManager and LoggerView."""
        self.file_manager = FileManager('downloads')
        self.view = LoggerView()
        self.semaphore = asyncio.Semaphore(5)  # Limit concurrent downloads

    async def process_file(self, file_name: str) -> None:
        """Process a single file asynchronously with limited concurrency.

        Args:
            file_name (str): The name of the file to download and extract.
        """
        try:
            async with self.semaphore:  # Limit concurrent downloads
                # Run the download in an async context
                zip_path = await self.file_manager.download_file(file_name)

            # Run the extraction using a thread pool
            await self.file_manager.extract_zip_in_thread(zip_path)
            self.view.log_info(f"Processed {file_name}")

        except FileNotFoundError:
            # Handle file not found errors
            self.view.log_error(f"{file_name} not found on the server.")

        except RetryError as error:
            # Handle retries exhausted errors
            if isinstance(error.last_attempt.exception(), FileNotFoundError):
                self.view.log_error(f"{file_name} not found after retries.")
            else:
                self.view.log_error(f"Unexpected retry error with {file_name}: {error}")

        except ConnectionError as error:
            self.view.log_error(f"Connection error with {file_name}: {error}")

        except Exception as error:
            self.view.log_error(f"Unexpected error with {file_name}: {error}")

    async def process_files(self) -> None:
        """Process all files in the file names list asynchronously.

        Creates asynchronous tasks for downloading and extracting
        each file and runs them in parallel.
        """
        self.file_manager.ensure_dir_exists()
        self.view.log_info("Starting to process files")

        # Create tasks for each file and run them in parallel
        tasks = [self.process_file(file_name) for file_name in self.FILE_NAMES]

        # Run tasks in parallel
        await asyncio.gather(*tasks)

        self.view.log_info("Finished processing files")

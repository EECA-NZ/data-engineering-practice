"""Module for managing file downloads and extraction asynchronously."""
import os
import aiohttp
import asyncio
import zipfile
import urllib3
import ssl
import aiofiles
from aiohttp import ClientTimeout, ClientResponseError
from aiohttp.client_exceptions import ClientConnectorError
from tenacity import retry, stop_after_attempt, wait_exponential
from concurrent.futures import ThreadPoolExecutor


class FileManager:
    """Class to manage file downloads and extraction asynchronously."""

    BASE_URI = "https://divvy-tripdata.s3.amazonaws.com/{}"

    def __init__(self, download_dir: str, verify_ssl: bool = False, max_workers: int = 5):
        """Initialize with the directory to store downloads and SSL verification option.

        Args:
            download_dir (str): Directory where the files will be downloaded.
            verify_ssl (bool): Whether to verify SSL certificates.
            max_workers (int): Maximum number of threads for parallel file extraction.
        """
        self.download_dir = download_dir
        self.verify_ssl = verify_ssl
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def ensure_dir_exists(self) -> None:
        """Ensure the directory for downloads exists."""
        os.makedirs(self.download_dir, exist_ok=True)

    def get_ssl_context(self) -> ssl.SSLContext:
        """Create and return an SSL context, optionally disabling SSL verification."""
        ssl_context = ssl.create_default_context()
        if not self.verify_ssl:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        return ssl_context

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def download_file(self, file_name: str) -> str:
        """Download the file in chunks from the constructed URI asynchronously.

        Args:
            file_name (str): The name of the file to download.

        Returns:
            str: The path to the downloaded file.
        """
        uri = self.BASE_URI.format(file_name)
        zip_path = os.path.join(self.download_dir, file_name)

        timeout = ClientTimeout(total=600)  # 10-minute total timeout
        ssl_context = self.get_ssl_context()

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(uri, ssl=ssl_context) as response:
                    if response.status == 404:
                        raise FileNotFoundError(f"File {file_name} not found on the server.")

                    response.raise_for_status()

                    async with aiofiles.open(zip_path, 'wb') as file:
                        async for chunk in response.content.iter_chunked(1024):
                            if chunk:
                                await file.write(chunk)

        except (ClientResponseError, ClientConnectorError, asyncio.TimeoutError) as error:
            raise ConnectionError(f"Error downloading {file_name}: {error}")

        return zip_path

    def extract_zip(self, file_path: str) -> None:
        """Extract the downloaded zip file and remove it.

        Args:
            file_path (str): The path to the zip file to extract.
        """
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(self.download_dir)
        os.remove(file_path)

    async def extract_zip_in_thread(self, file_path: str) -> None:
        """Run the extract_zip method in a separate thread."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.executor, self.extract_zip, file_path)

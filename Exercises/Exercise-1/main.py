"""Main script to initiate file processing asynchronously."""
import asyncio
from files.controller import FileController

async def main() -> None:
    """Main entry point of the application.

    This function initializes the FileController and processes the files.
    """
    controller = FileController()
    await controller.process_files()

if __name__ == "__main__":
    asyncio.run(main())

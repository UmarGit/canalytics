import asyncio
import websockets
import json
import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from storage.s3_loader import S3Loader

load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)


class AISCollectorAsync:
    """
    Collects AIS (Automatic Identification System) data asynchronously from a WebSocket stream and stores it in S3.
    
    Attributes:
    - api_key: Authentication token for the AIS API, obtained from the provided argument or the `AIS_TOKEN` environment variable.
    - s3_loader: Loader responsible for uploading data to S3, defaulting to a new `S3Loader` if none is supplied.
    - ws_url: WebSocket URL used to connect to the AIS streaming service.
    - message_count: Counter for the number of messages received, initialized to zero.
    """
    def __init__(self, api_key=None, s3_loader=None):
        """
        Initializes the AIS stream client.
        
        Args:
            self: The instance being initialized.
        
        Attributes:
            api_key: Authentication token for the AIS API, obtained from the provided argument or the `AIS_TOKEN` environment variable.
            s3_loader: Instance responsible for loading data from S3, defaulting to a new `S3Loader` if none is supplied.
            ws_url: WebSocket URL used to connect to the AIS streaming service.
            message_count: Counter for the number of messages received, initialized to zero.
        
        Returns:
            None
        """
        self.api_key = api_key or os.getenv("AIS_TOKEN")
        self.s3_loader = s3_loader or S3Loader()
        self.ws_url = "wss://stream.aisstream.io/v0/stream"
        self.message_count = 0

    async def connect_and_collect(self):
        """
        Connects to the AIS WebSocket, subscribes to a predefined set of geographic bounding boxes, and continuously receives position report messages. For each received position report, the method increments a message counter, logs relevant ship information, and delegates the message to `save_data` for persistence. Handles normal termination and unexpected errors with appropriate logging.
        
        Args:
            self: The instance of the class containing connection parameters such as `ws_url`, `api_key`, and the `message_count` counter.
        
        Returns:
            None
        """
        try:
            logger.info("Connecting to AIS WebSocket...")
            async with websockets.connect(self.ws_url) as websocket:
                subscribe_message = {
                    "APIKey": self.api_key,
                    "BoundingBoxes": [
                        [[29.5, 32.0], [31.5, 33.5]],
                        [[8.8, -79.7], [9.5, -79.2]],
                        [[35.8, -6.4], [36.3, -5.5]],
                        [[41.0, 28.9], [41.3, 29.1]],
                        [[1.0, 100.0], [3.0, 104.0]],
                    ],
                    "FilterMessageTypes": ["PositionReport"],
                }

                await websocket.send(json.dumps(subscribe_message))
                logger.info("Subscribed to AIS stream, waiting for messages...")

                async for message_json in websocket:
                    message = json.loads(message_json)
                    if message.get("MessageType") == "PositionReport":
                        self.message_count += 1
                        data = message["Message"]["PositionReport"]
                        time_stamp = message["MetaData"]["time_utc"]
                        logger.info(
                            f"[{self.message_count}] timestamp: {time_stamp}, Ship: {data['UserID']} Lat: {data['Latitude']} Lon: {data['Longitude']}"
                        )
                        await self.save_data(message)

        except (websockets.exceptions.ConnectionClosed, asyncio.CancelledError):
            logger.info(
                f"AIS collection stopped. Total messages processed: {self.message_count}"
            )
        except Exception as e:
            logger.error(f"Unexpected error in AIS collection: {str(e)}")

    async def save_data(self, data):
        """
        Saves the provided data as a JSON file to an S3 bucket with a timestamped key.
        
        Args:
            self: The instance of the class containing the S3 loader and logger.
            data: The data to be serialized to JSON and uploaded.
        
        Returns:
            None: The method logs the outcome of the upload but does not return a value.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        s3_key = f"raw/ais/ais_{timestamp}.json"

        # Upload directly to S3 instead of creating local file
        success = self.s3_loader.upload_json(data, s3_key)

        if success:
            logger.info(f"Saved to S3: {s3_key}")
        else:
            logger.error(f"Failed to save to S3: {s3_key}")

    def run(self):
        """
        Runs the data collection process by executing the asynchronous
        `connect_and_collect` coroutine using `asyncio.run`.
        
        Args:
            self: The instance of the class.
        
        Returns:
            The result returned by `connect_and_collect`, if any (commonly `None`).
        """
        asyncio.run(self.connect_and_collect())


if __name__ == "__main__":
    collector = AISCollectorAsync()
    collector.run()

"""`python -m onchain_engine` - runs the research engine. Live sending stays off
unless ALLOW_LIVE_TRANSACTIONS=true *and* a key is configured."""
import asyncio

from .engine import logger, main

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt caught, shutting down.")

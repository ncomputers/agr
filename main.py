# File: main.py

import asyncio
from exchanges.binance import binance_listener
from exchanges.bybit import bybit_listener
from exchanges.okx import okx_listener
from exchanges.coinbase import coinbase_listener

async def printer(queue):
    while True:
        trade = await queue.get()
        # Filter: only show trades with quantity > 0.5 BTC
        if trade['quantity'] > 0.5:
            print(f"[{trade['exchange']}] {trade['side']} - {trade['quantity']} BTC @ {trade['price']} USD at {trade['timestamp']}")

async def main():
    q = asyncio.Queue()
    await asyncio.gather(
        binance_listener(q),
        bybit_listener(q),
        okx_listener(q),
        coinbase_listener(q),
        printer(q)
    )

if __name__ == "__main__":
    asyncio.run(main())

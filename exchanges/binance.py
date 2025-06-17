import json
import asyncio
import websockets
from datetime import datetime

async def binance_listener(queue):
    uri = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    async with websockets.connect(uri) as ws:
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            await queue.put({
                'exchange': 'Binance',
                'price': float(data['p']),
                'quantity': float(data['q']),
                'side': 'SELL' if data['m'] else 'BUY',
                'timestamp': datetime.utcfromtimestamp(data['T']/1000).isoformat()
            })

# Test code (can be run independently)
if __name__ == "__main__":
    async def test():
        async def queue_printer(q):
            while True:
                item = await q.get()
                print(item)
        q = asyncio.Queue()
        await asyncio.gather(binance_listener(q), queue_printer(q))

    asyncio.run(test())
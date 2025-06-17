import json
import asyncio
import websockets
from datetime import datetime

async def bybit_listener(queue):
    uri = "wss://stream.bybit.com/v5/public/spot"
    async with websockets.connect(uri) as ws:
        subscribe_msg = {
            "op": "subscribe",
            "args": ["publicTrade.BTCUSDT"]
        }
        await ws.send(json.dumps(subscribe_msg))
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            if 'data' in data:
                for trade in data['data']:
                    await queue.put({
                        'exchange': 'Bybit',
                        'price': float(trade['p']),
                        'quantity': float(trade['v']),
                        'side': 'BUY' if trade['S'] == 'Buy' else 'SELL',
                        'timestamp': datetime.utcfromtimestamp(trade['T']/1000).isoformat()
                    })

if __name__ == "__main__":
    async def test():
        async def queue_printer(q):
            while True:
                item = await q.get()
                print(item)
        q = asyncio.Queue()
        await asyncio.gather(bybit_listener(q), queue_printer(q))

    asyncio.run(test())


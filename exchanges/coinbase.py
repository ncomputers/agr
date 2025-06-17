import json
import asyncio
import websockets

async def coinbase_listener(queue):
    uri = "wss://ws-feed.exchange.coinbase.com"
    subscribe_msg = {"type": "subscribe", "channels": [{"name": "matches", "product_ids": ["BTC-USD"]}]}

    while True:
        print("[CoinbasePro] Connecting to WebSocket...")
        try:
            async with websockets.connect(uri, open_timeout=20, ping_interval=20, ping_timeout=10) as ws:
                print("[CoinbasePro] Connected")
                await ws.send(json.dumps(subscribe_msg))
                print("[CoinbasePro] Subscribed")

                async for raw in ws:
                    data = json.loads(raw)
                    if data.get("type") == "match":
                        await queue.put({
                            'exchange': 'CoinbasePro',
                            'price': float(data['price']),
                            'quantity': float(data['size']),
                            'side': data['side'].upper(),
                            'timestamp': data['time']
                        })
        except Exception as e:
            print(f"[CoinbasePro] Error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

# Test code
if __name__ == "__main__":
    async def test():
        async def printer(q):
            while True:
                print(await q.get())
        q = asyncio.Queue()
        await asyncio.gather(coinbase_listener(q), printer(q))

    asyncio.run(test())
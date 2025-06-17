import json
import asyncio
import websockets

async def okx_listener(queue):
    uri = "wss://ws.okx.com:8443/ws/v5/public"
    subscribe_msg = {"op": "subscribe", "args": [{"channel": "trades", "instId": "BTC-USDT"}]}

    backoff = 5
    while True:
        print(f"[OKX] Connecting... (backoff={backoff}s)")
        try:
            async with websockets.connect(
                uri,
                open_timeout=20,
                ping_interval=20,
                ping_timeout=10,
                compression='deflate'
            ) as ws:
                print("[OKX] Connected")
                await ws.send(json.dumps(subscribe_msg))
                print("[OKX] Subscribed, awaiting messages...")
                backoff = 5

                async for raw in ws:
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        print(f"[OKX] Invalid JSON: {raw}")
                        continue

                    if data.get('arg', {}).get('channel') == 'trades' and 'data' in data:
                        for trade in data['data']:
                            await queue.put({
                                'exchange': 'OKX',
                                'price': float(trade['px']),
                                'quantity': float(trade['sz']),
                                'side': 'BUY' if trade['side'] == 'buy' else 'SELL',
                                'timestamp': trade['ts']
                            })
        except Exception as e:
            print(f"[OKX] Error: {e}. Retrying in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# Test code (independent)
if __name__ == "__main__":
    async def test():
        async def printer(q):
            while True:
                print(await q.get())
        q = asyncio.Queue()
        await asyncio.gather(okx_listener(q), printer(q))

    asyncio.run(test())
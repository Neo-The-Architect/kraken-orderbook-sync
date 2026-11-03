"""Explicit opt-in public-data client. No authentication, order or trading methods."""
import argparse
import asyncio
import json
from orderbook import OrderBook,ResyncRequired,loads

class Client:
    def __init__(self,book,*,idle_timeout=30):self.book=book;self.idle_timeout=idle_timeout;self.reconnections=0
    def subscription(self):
        return {'method':'subscribe','params':{'channel':'book','symbol':[self.book.symbol],'depth':self.book.depth,'snapshot':True}}
    async def run(self,connector,on_book,*,max_sessions=None,sleep=asyncio.sleep):
        session=0
        while max_sessions is None or session<max_sessions:
            session+=1;self.book.reset()
            try:
                async with connector() as ws:
                    await ws.send(json.dumps(self.subscription()))
                    while True:
                        raw=await asyncio.wait_for(ws.recv(),timeout=self.idle_timeout)
                        message=loads(raw)
                        if message.get('method')=='subscribe':
                            if message.get('success') is not True:raise ResyncRequired('Subscription refused')
                            continue
                        if message.get('channel') in ('heartbeat','status'):continue
                        view=self.book.apply(message)
                        if await on_book(view):return
            except asyncio.CancelledError:
                self.book.invalidate();raise
            except Exception:
                self.book.invalidate();self.reconnections+=1
                if max_sessions is None or session<max_sessions:await sleep(min(30,2**min(session-1,5)))

async def live(symbol,depth):
    from websockets.asyncio.client import connect
    book=OrderBook(symbol,depth)
    client=Client(book)
    def connector():return connect('wss://ws.kraken.com/v2',proxy=None,max_size=1048576,max_queue=16,ping_interval=20,ping_timeout=20)
    async def show(view):
        print(json.dumps({'symbol':view['symbol'],'state':view['state'],'best_ask':str(view['asks'][0].price),'best_bid':str(view['bids'][0].price)}))
        return False
    await client.run(connector,show)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--live',action='store_true');p.add_argument('--symbol',default='BTC/USD');p.add_argument('--depth',type=int,default=10);a=p.parse_args()
    if not a.live:p.exit(2,'Live connection is disabled by default; run tests or explicitly pass --live.\n')
    asyncio.run(live(a.symbol,a.depth))

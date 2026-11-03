import asyncio
import copy
from decimal import Decimal
import json
import unittest
from orderbook import *
from client import Client

# Published Kraken v2 checksum example: expected CRC32 is independently specified.
def reference():
    bids=[('45283.5','0.10000000'),('45283.4','1.54582015'),('45282.1','0.10000000'),('45281.0','0.10000000'),('45280.3','1.54592586'),('45279.0','0.07990000'),('45277.6','0.03310103'),('45277.5','0.30000000'),('45277.3','1.54602737'),('45276.6','0.15445238')]
    asks=[('45285.2','0.00100000'),('45286.4','1.54571953'),('45286.6','1.54571109'),('45289.6','1.54560911'),('45290.2','0.15890660'),('45291.8','1.54553491'),('45294.7','0.04454749'),('45296.1','0.35380000'),('45297.5','0.09945542'),('45299.5','0.18772827')]
    return {'channel':'book','type':'snapshot','data':[{'symbol':'BTC/USD','asks':[{'price':p,'qty':q} for p,q in asks],'bids':[{'price':p,'qty':q} for p,q in bids],'checksum':3310070434}]}

def updated(book,asks=None,bids=None):
    row={'symbol':book.symbol,'asks':asks or [],'bids':bids or []}
    maps={'asks':dict(book.asks),'bids':dict(book.bids)}
    for side in maps:
        for item in row[side]:
            p=Decimal(str(item['price']));q=Decimal(str(item['qty']))
            if q==0:maps[side].pop(p,None)
            else:maps[side][p]=Level(p,q)
        ordered=sorted(maps[side].values(),key=lambda x:x.price,reverse=side=='bids')[:book.depth]
        maps[side]={x.price:x for x in ordered}
    row['checksum']=checksum(list(maps['asks'].values()),list(maps['bids'].values()))
    return {'channel':'book','type':'update','data':[row]}

class BookTests(unittest.TestCase):
    def setUp(self):self.book=OrderBook();self.book.apply(reference())
    def test_reference_crc(self):self.assertEqual(checksum(list(self.book.asks.values()),list(self.book.bids.values())),3310070434)
    def test_snapshot_synced(self):self.assertEqual(self.book.state,State.SYNCED)
    def test_decimal_decoder_preserves_precision(self):self.assertEqual(str(loads('{"p":0.10000000}')['p']),'0.10000000')
    def test_binary_float_rejected(self):
        with self.assertRaises(ResyncRequired):number(0.1)
    def test_zero_quantity_removes_level(self):
        self.book.apply(updated(self.book,asks=[{'price':'45285.2','qty':'0'}]));self.assertNotIn(Decimal('45285.2'),self.book.asks)
    def test_multiple_updates_same_level_are_ordered(self):
        self.book.apply(updated(self.book,asks=[{'price':'45285.2','qty':'1.0000'},{'price':'45285.2','qty':'2.0000'}]))
        self.assertEqual(str(self.book.asks[Decimal('45285.2')].qty),'2.0000')
    def test_depth_pruned(self):
        self.book.apply(updated(self.book,asks=[{'price':'45284.0','qty':'0.10000000'}]));self.assertEqual(len(self.book.asks),10);self.assertNotIn(Decimal('45299.5'),self.book.asks)
    def test_update_before_snapshot_rejected(self):
        book=OrderBook()
        with self.assertRaises(ResyncRequired):book.apply(updated(self.book))
    def test_bad_crc_invalidates_and_clears_book(self):
        m=updated(self.book);m['data'][0]['checksum']^=1
        with self.assertRaises(ResyncRequired):self.book.apply(m)
        self.assertEqual(self.book.state,State.STALE);self.assertFalse(self.book.asks)
    def test_stale_view_refused(self):
        clock=[0];b=OrderBook(clock=lambda:clock[0],max_age=5);b.apply(reference());clock[0]=6
        with self.assertRaises(ResyncRequired):b.view()
    def test_crossed_book_rejected(self):
        m=reference();m['data'][0]['bids'][0]['price']='50000'
        with self.assertRaises(ResyncRequired):self.book.apply(m)
    def test_duplicate_snapshot_level_rejected(self):
        m=reference();m['data'][0]['asks'].append(m['data'][0]['asks'][0])
        with self.assertRaises(ResyncRequired):self.book.apply(m)
    def test_symbol_mismatch_rejected(self):
        m=reference();m['data'][0]['symbol']='ETH/USD'
        with self.assertRaises(ResyncRequired):self.book.apply(m)
    def test_missing_checksum_rejected(self):
        m=reference();del m['data'][0]['checksum']
        with self.assertRaises(ResyncRequired):self.book.apply(m)
    def test_nonfinite_rejected(self):
        with self.assertRaises(ResyncRequired):loads('{"price":NaN}')
    def test_duplicate_json_key_rejected(self):
        with self.assertRaises(ResyncRequired):loads('{"a":1,"a":2}')
    def test_no_sequence_number_in_v2_contract(self):
        m=reference();m['data'][0]['sequence']=9
        with self.assertRaises(ResyncRequired):self.book.apply(m)
    def test_empty_book_rejected(self):
        m=reference();m['data'][0]['asks']=[]
        with self.assertRaises(ResyncRequired):self.book.apply(m)
    def test_reset_requires_fresh_snapshot(self):
        self.book.reset();self.assertEqual(self.book.state,State.AWAITING_SNAPSHOT)
        with self.assertRaises(ResyncRequired):self.book.view()
    def test_top_ten_scope_is_explicit(self):
        asks=list(self.book.asks.values());self.assertEqual(checksum(asks+[Level(Decimal('99999'),Decimal('3'))],list(self.book.bids.values())),3310070434)

class FakeSocket:
    def __init__(self,messages):self.messages=iter(messages);self.sent=[]
    async def __aenter__(self):return self
    async def __aexit__(self,*args):return False
    async def send(self,message):self.sent.append(json.loads(message))
    async def recv(self):
        try:return json.dumps(next(self.messages))
        except StopIteration:raise EOFError

class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_resubscribe_snapshot_after_checksum_failure(self):
        bad=reference();bad['data'][0]['checksum']^=1
        sockets=[FakeSocket([reference(),bad]),FakeSocket([reference()])];pending=iter(sockets)
        book=OrderBook();client=Client(book);published=[]
        async def callback(view):published.append(view);return len(published)==2
        async def no_sleep(_):return
        await client.run(lambda:next(pending),callback,max_sessions=2,sleep=no_sleep)
        self.assertEqual(client.reconnections,1);self.assertEqual(len(published),2)
        self.assertTrue(all(s.sent[0]['params']['snapshot'] for s in sockets))
    async def test_disconnect_recovery_requires_snapshot(self):
        sockets=iter([FakeSocket([]),FakeSocket([reference()])]);client=Client(OrderBook());count=[]
        async def callback(view):count.append(view);return True
        async def no_sleep(_):return
        await client.run(lambda:next(sockets),callback,max_sessions=2,sleep=no_sleep)
        self.assertEqual(client.reconnections,1);self.assertEqual(len(count),1)
    async def test_subscription_failure_never_publishes(self):
        client=Client(OrderBook());published=[]
        async def callback(view):published.append(view);return True
        await client.run(lambda:FakeSocket([{'method':'subscribe','success':False}]),callback,max_sessions=1)
        self.assertFalse(published);self.assertEqual(client.book.state,State.STALE)

if __name__=='__main__':unittest.main(verbosity=2)

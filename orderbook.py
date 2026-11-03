"""Kraken v2 L2 checksum synchronization. Importing never opens a connection."""
from dataclasses import dataclass
from decimal import Decimal,InvalidOperation
from enum import Enum
import json
import re
import time
import zlib

class ResyncRequired(ValueError):pass
class State(str,Enum):
    AWAITING_SNAPSHOT='AWAITING_SNAPSHOT'
    SYNCED='SYNCED'
    STALE='STALE'

def loads(raw):
    if not isinstance(raw,(str,bytes)) or len(raw)>1048576:raise ResyncRequired('Frame size/type rejected')
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise ResyncRequired('Duplicate JSON key')
            out[k]=v
        return out
    try:return json.loads(raw,parse_float=Decimal,parse_int=int,object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(ResyncRequired('Nonfinite JSON number')))
    except (ValueError,UnicodeError,RecursionError) as e:raise ResyncRequired('Malformed frame') from e

def number(value,zero=False):
    if isinstance(value,bool) or isinstance(value,float) or not isinstance(value,(str,int,Decimal)):
        raise ResyncRequired('Use decimal/string JSON decoding; binary floats rejected')
    text=str(value)
    if len(text)>128 or not re.fullmatch(r'[0-9]+(?:\.[0-9]+)?(?:[Ee][+-]?[0-9]+)?',text):raise ResyncRequired('Invalid numeric field')
    try:result=Decimal(text)
    except InvalidOperation as e:raise ResyncRequired('Invalid decimal') from e
    if not result.is_finite() or result<0 or (result==0 and not zero) or abs(result.as_tuple().exponent)>100:
        raise ResyncRequired('Numeric bounds rejected')
    return result

@dataclass(frozen=True)
class Level:
    price:Decimal
    qty:Decimal

def checksum(asks,bids):
    def component(value):return format(value,'f').replace('.','').lstrip('0')
    ordered=sorted(asks,key=lambda x:x.price)[:10]+sorted(bids,key=lambda x:x.price,reverse=True)[:10]
    wire=''.join(component(x.price)+component(x.qty) for x in ordered)
    return zlib.crc32(wire.encode('ascii')) & 0xffffffff

class OrderBook:
    def __init__(self,symbol='BTC/USD',depth=10,*,clock=time.monotonic,max_age=30):
        if depth not in (10,25,100,500,1000) or not re.fullmatch(r'[A-Z0-9.]{2,12}/[A-Z0-9.]{2,12}',symbol):raise ValueError('Unsupported subscription')
        if max_age<=0:raise ValueError('Positive freshness limit required')
        self.symbol=symbol;self.depth=depth;self.clock=clock;self.max_age=max_age
        self.state=State.AWAITING_SNAPSHOT;self.asks={};self.bids={};self.updated_at=None
    def invalidate(self):
        self.state=State.STALE;self.asks={};self.bids={};self.updated_at=None
    def reset(self):
        self.invalidate();self.state=State.AWAITING_SNAPSHOT
    def apply(self,message):
        try:
            if not isinstance(message,dict) or message.get('channel')!='book':raise ResyncRequired('Expected book message')
            kind=message.get('type');data=message.get('data')
            if kind not in ('snapshot','update') or not isinstance(data,list) or len(data)!=1:raise ResyncRequired('Unsupported frame shape')
            row=data[0]
            if row.get('symbol')!=self.symbol:raise ResyncRequired('Symbol mismatch')
            if 'sequence' in row or 'sequence' in message:raise ResyncRequired('No sequence field is defined for this v2 book contract')
            if kind=='update' and self.state!=State.SYNCED:raise ResyncRequired('Snapshot required before updates')
            maps={'asks':{} if kind=='snapshot' else dict(self.asks),'bids':{} if kind=='snapshot' else dict(self.bids)}
            for side in ('asks','bids'):
                updates=row.get(side,[])
                if not isinstance(updates,list) or len(updates)>max(100,4*self.depth):raise ResyncRequired('Level count exceeded')
                seen=set()
                for item in updates:
                    if not isinstance(item,dict) or set(item)!={'price','qty'}:raise ResyncRequired('Invalid price level')
                    price=number(item['price']);qty=number(item['qty'],zero=kind=='update')
                    if kind=='snapshot' and price in seen:raise ResyncRequired('Duplicate snapshot level')
                    seen.add(price)
                    if qty==0:maps[side].pop(price,None)
                    else:maps[side][price]=Level(price,qty)
                ordered=sorted(maps[side].values(),key=lambda x:x.price,reverse=side=='bids')[:self.depth]
                maps[side]={item.price:item for item in ordered}
            if not maps['asks'] or not maps['bids'] or max(maps['bids'])>=min(maps['asks']):raise ResyncRequired('Empty or crossed book')
            expected=row.get('checksum')
            if isinstance(expected,bool) or not isinstance(expected,int) or not 0<=expected<=0xffffffff:raise ResyncRequired('Missing/invalid checksum')
            if checksum(list(maps['asks'].values()),list(maps['bids'].values()))!=expected:raise ResyncRequired('Checksum mismatch')
            self.asks=maps['asks'];self.bids=maps['bids'];self.updated_at=self.clock();self.state=State.SYNCED
            return self.view()
        except (ValueError,TypeError,KeyError,AttributeError) as e:
            self.invalidate()
            raise ResyncRequired('Book invalidated; reconnect and request snapshot') from e
    def view(self):
        if self.state!=State.SYNCED or self.updated_at is None or self.clock()-self.updated_at>self.max_age:
            self.invalidate();raise ResyncRequired('Book unavailable or stale')
        return {'symbol':self.symbol,'state':self.state.value,
            'asks':tuple(sorted(self.asks.values(),key=lambda x:x.price)),
            'bids':tuple(sorted(self.bids.values(),key=lambda x:x.price,reverse=True))}

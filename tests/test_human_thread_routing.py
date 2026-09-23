"""Human replies use native threads and explicit expired-transcript context."""
import asyncio
from dataclasses import dataclass, replace
from types import SimpleNamespace as NS

import discord
import pytest

from pebble_bridge.human_threads import route_human_event
from pebble_bridge.repository import Repository
from pebble_bridge.relay_store import RelayStore
from test_relay_store import SCOPE

HUMAN_MESSAGE = '623456789012345678'
CARD = '523456789012345678'

@dataclass
class Source:
    chat_id: str = SCOPE['channel_id']
    thread_id: str | None = None
    parent_chat_id: str | None = None
    chat_type: str = 'group'
    chat_name: str = 'ring'
    auto_thread_created: bool = False
    auto_thread_initial_name: str | None = None

@dataclass
class Event:
    source: Source
    text: str = 'Please explain the idea in this recording'
    message_id: str = HUMAN_MESSAGE
    raw_message: object = None
    reply_to_message_id: str | None = CARD
    reply_to_text: str | None = None
    channel_context: str | None = 'unrelated parent history'
    metadata: object = None


def setup(tmp_path, *, expired=True, transcript='SYNTHETIC EXPIRED TRANSCRIPT'):
    repo = Repository(tmp_path)
    repo.persist_capture(client='synthetic', recorded_at=1000, received_at=1000,
        transcript=transcript, audio=b'synthetic', forced_new_topic=True)
    repo.connection.close()
    with RelayStore(tmp_path) as store:
        req = store.claim_card(**SCOPE, now_ms=2000, thread_mode=True)
        req = store.bind_card(req.capture_id, CARD)
        if expired:
            store.expire_due(**SCOPE, now_ms=req.expires_at)
    guild = NS(id=int(SCOPE['guild_id']), get_thread=lambda _: None)
    parent = object.__new__(discord.TextChannel)
    parent.id, parent.guild, parent.name = int(SCOPE['channel_id']), guild, 'ring'
    thread = discord.Thread(guild=guild, state=NS(), data={
        'id': HUMAN_MESSAGE, 'parent_id': SCOPE['channel_id'], 'owner_id': SCOPE['approver_id'],
        'name': 'new conversation', 'type': 11, 'message_count': 0, 'member_count': 1,
        'rate_limit_per_user': 0, 'thread_metadata': {'archived':False,
        'auto_archive_duration':1440,'archive_timestamp':'2026-01-01T00:00:00+00:00','locked':False}})
    created=[]
    class HumanMessage(discord.Message):
        async def create_thread(self, **kwargs):
            created.append(kwargs)
            return thread
    raw=object.__new__(HumanMessage)
    raw.id, raw.channel, raw.guild = int(HUMAN_MESSAGE), parent, guild
    raw._thread=None
    raw.author=NS(id=int(SCOPE['approver_id']),bot=False)
    raw.content='Please explain the idea in this recording'
    raw.reference=discord.MessageReference(message_id=int(CARD),channel_id=parent.id,guild_id=guild.id)
    event=Event(Source(),raw_message=raw)
    service=NS(scope=SCOPE,authenticated=lambda e: True,store=lambda:RelayStore(tmp_path),
        port=NS(adapter=NS(_format_thread_chat_name=lambda t:'ring / conversation',
            _threads=NS(mark=lambda ident:None))))
    return service,event,created,thread,req


def test_expired_reply_routes_to_human_thread_and_includes_context_without_reexecution(tmp_path):
    service,event,created,thread,req=setup(tmp_path)
    with service.store() as store:
        before=(store.get(req.capture_id),store.get_route(req.capture_id))
    routed=asyncio.run(route_human_event(service,event))
    assert len(created)==1
    assert routed.source.chat_id == HUMAN_MESSAGE
    assert routed.source.thread_id == HUMAN_MESSAGE
    assert routed.source.parent_chat_id == SCOPE['channel_id']
    assert routed.source.chat_type == 'thread'
    assert routed.text == event.text
    assert 'SYNTHETIC EXPIRED TRANSCRIPT' in routed.reply_to_text
    assert 'context' in routed.reply_to_text.lower()
    assert routed.channel_context is None
    with service.store() as store:
        assert (store.get(req.capture_id),store.get_route(req.capture_id)) == before
        assert store.db.execute('SELECT count(*) FROM relay_audio_grants').fetchone()[0]==0


def test_plain_parent_question_also_routes_and_does_not_import_capture(tmp_path):
    service,event,created,thread,_=setup(tmp_path)
    event.raw_message.reference=None
    event.reply_to_message_id=None
    routed=asyncio.run(route_human_event(service,event))
    assert len(created)==1 and routed.source.chat_id==HUMAN_MESSAGE
    assert routed.reply_to_text is None


def test_existing_thread_without_reference_stays_identical(tmp_path):
    service,event,created,thread,_=setup(tmp_path)
    event=replace(event,source=Source(chat_id=HUMAN_MESSAGE,thread_id=HUMAN_MESSAGE,
        parent_chat_id=SCOPE['channel_id'],chat_type='thread'),reply_to_message_id=None)
    event.raw_message.reference=None
    assert asyncio.run(route_human_event(service,event)) is event
    assert not created


@pytest.mark.parametrize('bad',['author','bot','message','reference','parent','guild'])
def test_unverified_human_or_reply_cannot_create_thread_or_import_transcript(tmp_path,bad):
    service,event,created,thread,_=setup(tmp_path)
    if bad=='author': event.raw_message.author.id+=1
    if bad=='bot': event.raw_message.author.bot=True
    if bad=='message': event.raw_message.id+=1
    if bad=='reference': event.reply_to_message_id='723456789012345678'
    if bad=='parent': event.raw_message.channel.id+=1
    if bad=='guild': event.raw_message.guild.id+=1
    with pytest.raises((ValueError,PermissionError)):
        asyncio.run(route_human_event(service,event))
    assert not created and event.reply_to_text is None


def test_thread_create_failure_never_falls_back_to_parent(tmp_path):
    service,event,created,thread,_=setup(tmp_path)
    async def fail(self, **kwargs): raise TimeoutError('synthetic timeout')
    type(event.raw_message).create_thread=fail
    with pytest.raises(TimeoutError): asyncio.run(route_human_event(service,event))
    assert event.source.chat_id==SCOPE['channel_id'] and event.reply_to_text is None


def test_pending_card_is_not_implicitly_approved_or_imported(tmp_path):
    service,event,created,thread,req=setup(tmp_path,expired=False)
    routed=asyncio.run(route_human_event(service,event))
    assert routed.reply_to_text is None
    with service.store() as store: assert store.get(req.capture_id).state=='pending'


def test_long_expired_transcript_is_preserved_in_full(tmp_path):
    transcript='SYNTHETIC CONTEXT ' * 200 + 'END OF TRANSCRIPT'
    service,event,created,thread,_=setup(tmp_path,transcript=transcript)
    routed=asyncio.run(route_human_event(service,event))
    assert transcript in routed.reply_to_text
    assert routed.text==event.text


def test_wrong_created_thread_never_reaches_native_conversation(tmp_path):
    service,event,created,thread,_=setup(tmp_path)
    thread.parent_id+=1
    with pytest.raises(PermissionError): asyncio.run(route_human_event(service,event))
    assert event.source.chat_id==SCOPE['channel_id']


def test_database_failure_happens_before_thread_creation(tmp_path):
    service,event,created,thread,_=setup(tmp_path)
    def unavailable(): raise RuntimeError('synthetic unavailable state')
    service.store=unavailable
    with pytest.raises(RuntimeError): asyncio.run(route_human_event(service,event))
    assert not created

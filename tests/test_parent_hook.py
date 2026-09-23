"""Parent messages reach the async guard; unauthenticated input is rejected."""
from types import SimpleNamespace as NS

from pebble_bridge import plugin

SCOPE = {'guild_id': '323456789012345678',
         'channel_id': '223456789012345678',
         'approver_id': '123456789012345678'}


def test_parent_hook_defers_authenticated_input_and_blocks_other_users(tmp_path, monkeypatch):
    notices = []

    class Service:
        def __init__(self, *args, **kwargs):
            self.guard = NS(_closed=False)
            self.known_thread_ids = set()
            self.rendered = {}

        async def on_ready(self):
            self.ready_callback()

        def start(self):
            pass

        def authenticated(self, event):
            return event.source.user_id == SCOPE['approver_id']

        def allow_event(self, event):
            return False

        def offer_notice(self, event):
            notices.append(event.message_id)

    monkeypatch.setattr(plugin, 'RingService', Service)
    monkeypatch.setattr(plugin, 'external_state_path', lambda path: True)
    monkeypatch.setattr(plugin, 'register_management', lambda *args: None)
    monkeypatch.setattr(plugin, 'register_audio_tool', lambda *args: None)
    hooks, handlers = {}, {}
    settings = SCOPE | {'data_dir': str(tmp_path), 'audio_retention_ms': 3600000}
    ctx = NS(get_config=settings.get, register_skill=lambda **kwargs: None,
             register_hook=lambda name, fn: hooks.update({name: fn}),
             register_platform_handler=lambda name, fn: handlers.update({name: fn}))
    plugin.register(ctx)
    platform = type('Platform', (), {'value': 'discord'})()
    adapter = object()
    native = NS(add_listener=lambda callback, event: None, is_ready=lambda: True)
    handlers['discord'](native, adapter)
    source = NS(platform=platform, scope_id=SCOPE['guild_id'],
                chat_id=SCOPE['channel_id'], parent_chat_id=None,
                user_id=SCOPE['approver_id'], is_bot=False)
    event = NS(source=source, internal=False, allow_gateway_control=True,
               message_id='923456789012345678', text='Synthetic question',
               get_command=lambda: None)
    gateway = NS(adapters={platform: adapter})
    assert hooks['pre_gateway_dispatch'](event=event, gateway=gateway) is None
    assert not notices
    source.user_id = '823456789012345678'
    assert hooks['pre_gateway_dispatch'](event=event, gateway=gateway)['action'] == 'skip'
    assert notices == [event.message_id]

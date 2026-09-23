"""Native human conversations, separate from one-shot capture execution.

Only an authenticated, explicit reply to an expired card imports its transcript.
No capture claims, approvals, audio grants or capture-route identities are written.
"""
import asyncio
from dataclasses import replace
import discord


async def route_human_event(service, event):
    if not service.authenticated(event):
        return event  # Synthetic capture dispatch retains its exact identity.
    source = event.source
    parent_input = source.chat_id == service.scope['channel_id']
    reference_id = getattr(event, 'reply_to_message_id', None)
    if not parent_input and reference_id is None:
        return event
    raw = getattr(event, 'raw_message', None)
    scope = service.scope
    if (not isinstance(raw, discord.Message)
            or str(raw.id) != event.message_id
            or raw.author.bot is not False
            or str(raw.author.id) != scope['approver_id']
            or raw.guild is None or str(raw.guild.id) != scope['guild_id']
            or str(raw.channel.id) != source.chat_id
            or (parent_input and not isinstance(raw.channel, discord.TextChannel))):
        raise PermissionError('unverified Ring human message')
    reference = raw.reference
    if reference_id is not None and (
            reference is None or str(reference.message_id) != reference_id):
        raise PermissionError('unverified Ring reply')
    reply_context = None
    if reference_id is not None and str(reference.channel_id) == scope['channel_id']:
        with service.store() as store:
            request = store.card_request(reference_id, **scope)
        if request is not None and request.state == 'expired':
            reply_context = (
                'Expired Ring transcript supplied as context by your manual reply. '
                'Answer the new human message; this is not a replay of the capture.\n'
                '<ring_transcript_context>\n' + request.transcript +
                '\n</ring_transcript_context>')
    if not parent_input:
        return replace(event, reply_to_text=reply_context) if reply_context else event
    # Anchor on the NEW human message, never the expired capture card. Native
    # message-based threads have the starter's ID, so a read-confirmed existing
    # thread can be reused without a second create attempt.
    async with asyncio.timeout(10):
        thread = raw.thread
        if thread is None:
            title = ' '.join(event.text.split())[:90] or 'Ring conversation'
            thread = await raw.create_thread(name=title, auto_archive_duration=1440)
    if (not isinstance(thread, discord.Thread)
            or thread.type != discord.ChannelType.public_thread
            or str(thread.id) != event.message_id
            or str(thread.parent_id) != scope['channel_id']
            or thread.guild is None or str(thread.guild.id) != scope['guild_id']
            or thread.archived or thread.locked):
        raise PermissionError('unverified Ring conversation thread')
    service.port.adapter._threads.mark(str(thread.id))
    target = replace(source, chat_id=str(thread.id), thread_id=str(thread.id),
        parent_chat_id=scope['channel_id'], chat_type='thread',
        chat_name=service.port.adapter._format_thread_chat_name(thread),
        auto_thread_created=True, auto_thread_initial_name=thread.name)
    return replace(event, source=target, channel_context=None,
                   reply_to_text=reply_context or event.reply_to_text)

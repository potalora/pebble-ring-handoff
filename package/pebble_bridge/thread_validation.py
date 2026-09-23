"""Pure destination evidence checks, not durable approval or readiness."""

import discord


class DestinationRejected(Exception):
    """Content-free, closed diagnostic vocabulary."""
    def __init__(self, failure_code='mismatch'):
        self.failure_code = failure_code if failure_code in {'mismatch', 'unavailable', 'rejected'} else 'rejected'
        super().__init__(self.failure_code)


def validate_parent_card(message, request, *, scope, bot_id):
    if not isinstance(message, discord.Message) or not isinstance(message.channel, discord.TextChannel):
        raise DestinationRejected()
    _scope(request, scope)
    if (message.guild is None or str(message.guild.id) != scope['guild_id']
            or str(message.channel.guild.id) != scope['guild_id']
            or str(message.channel.id) != scope['channel_id']
            or str(message.id) != request.message_id):
        raise DestinationRejected()
    if (type(bot_id) is not int or message.author.id != bot_id or not message.author.bot):
        raise DestinationRejected()
    if message.content or message.attachments or len(message.embeds) != 1:
        raise DestinationRejected()
    embed = message.embeds[0]
    if embed.title != f'Ring capture {request.capture_id}' or embed.description != request.transcript:
        raise DestinationRejected()
    from .discord_ui import WARNING
    if embed.footer.text != WARNING:
        raise DestinationRejected()
    for name, value in (
        ('Approval expires', f'<t:{request.expires_at // 1000}:R>'),
        ('Private audio available until', f'<t:{request.audio_expires_at // 1000}:R>'),
    ):
        if [field.value for field in embed.fields if field.name == name] != [value]:
            raise DestinationRejected()
    labels = {'Awaiting approval', 'Queued', 'Running', 'Turn finished',
              'Uncertain — manual review required', 'Failed — manual review required',
              'Expired', 'Cancelled by reset', 'Rejected'}
    states = [field.value for field in embed.fields if field.name == 'State']
    from .discord_ui import (CONVERSATION_LABELS, CREATION_STOPPED_LABEL,
                             PREFLIGHT_INTERRUPTED_LABEL, WAITING_LABEL)
    labels.add(WAITING_LABEL)
    conversations = [field.value for field in embed.fields if field.name == 'Conversation']
    estimates = [field.value for field in embed.fields if field.name == 'Next attempt']
    if estimates:
        import re
        match = re.fullmatch(r'<t:([0-9]{1,16}):R>', estimates[0])
        if (len(estimates) != 1 or not match or states != [WAITING_LABEL]
                or not 0 <= int(match[1]) < request.expires_at // 1000):
            raise DestinationRejected()
    if (len(embed.fields) not in (3, 4, 5) or len(states) != 1 or states[0] not in labels
            or (len(embed.fields) == 3 and conversations)
            or (len(embed.fields) >= 4 and (len(conversations) != 1
                or conversations[0] not in {*CONVERSATION_LABELS.values(),
                                            CREATION_STOPPED_LABEL, PREFLIGHT_INTERRUPTED_LABEL}))
            or (len(embed.fields) == 5) != bool(estimates)):
        raise DestinationRejected()
    return message


def validate_thread(thread, request, *, scope, bot_member):
    if not isinstance(thread, discord.Thread):
        raise DestinationRejected()
    _scope(request, scope)
    if (thread.type != discord.ChannelType.public_thread
            or str(thread.id) != request.message_id
            or str(thread.parent_id) != scope['channel_id']
            or thread.guild is None or str(thread.guild.id) != scope['guild_id']):
        raise DestinationRejected()
    if thread.archived or thread.locked:
        raise DestinationRejected('rejected')
    if (not isinstance(bot_member, discord.Member) or not bot_member.bot
            or bot_member.guild is None or str(bot_member.guild.id) != scope['guild_id']):
        raise DestinationRejected('unavailable')
    parent = thread.parent
    if (not isinstance(parent, discord.TextChannel) or str(parent.id) != scope['channel_id']
            or parent.guild is None or str(parent.guild.id) != scope['guild_id']):
        raise DestinationRejected('unavailable')
    try:
        permissions = thread.permissions_for(bot_member)
    except (discord.ClientException, AttributeError, KeyError, TypeError):
        raise DestinationRejected('unavailable') from None
    if not (permissions.view_channel and permissions.read_message_history
            and permissions.send_messages_in_threads):
        raise DestinationRejected('rejected')
    return thread


def _scope(request, scope):
    import re
    if not isinstance(request.message_id, str) or not re.fullmatch(r'[0-9]{17,20}', request.message_id):
        raise DestinationRejected()
    if not isinstance(scope, dict) or any(
        not isinstance(scope.get(key), str) or not scope[key]
        or getattr(request, key, None) != scope[key]
        for key in ('guild_id', 'channel_id', 'approver_id')
    ):
        raise DestinationRejected()

"""Fixed native-session creation and known-ID GETs; not integration-ready."""
import discord
import aiohttp
import math
import asyncio
from contextlib import AsyncExitStack, asynccontextmanager


class ClosedTransport(RuntimeError):
    def __init__(self, failure_code, uncertain=False, message=None):
        self.failure_code = failure_code
        self.uncertain = uncertain
        super().__init__(message or failure_code)


class SafeCooldown(ClosedTransport):
    """A pre-send refusal or complete Discord denial, never ambiguous I/O."""
    def __init__(self, failure_code, *, operation, scope, retry_after):
        if (operation not in ('create', 'card', 'thread')
                or scope not in ('route', 'shared', 'global')
                or failure_code not in ('route_cooldown', 'global_cooldown',
                    'http_429_route', 'http_429_shared', 'http_429_global')
                or (retry_after is not None and (type(retry_after) not in (int, float)
                    or not math.isfinite(retry_after) or not 0 < retry_after <= 2**31))):
            raise ValueError('invalid cooldown')
        self.operation, self.scope, self.retry_after = operation, scope, retry_after
        super().__init__(failure_code)


def _snowflake(value):
    # Internal snowflakes may be positive integers or canonical decimal strings.
    return (type(value) is int and 0 < value < 2**64) or (
        type(value) is str and 1 <= len(value) <= 20
        and value.isascii() and value.isdecimal()
        and not value.startswith('0') and int(value) < 2**64)


class _Reservation:
    """Keep native exit in-task, bounded even during timeout unwinding."""
    def __init__(self, bucket, deadline):
        self.bucket = bucket
        self.deadline = deadline

    async def __aenter__(self):
        return await self.bucket.__aenter__()

    async def __aexit__(self, exc_type, exc, traceback):
        if type(exc) is SafeCooldown:
            # Pinned 2.7.1 release-only bookkeeping. Native exit additionally
            # sleeps/resets the bucket, which would erase a durable long wait.
            # Acquire succeeded before this context was entered; no await here.
            if self.bucket.outgoing < 1:
                raise ClosedTransport('accounting_error') from None
            self.bucket.outgoing -= 1
            if exc.failure_code in ('global_cooldown', 'route_cooldown'):
                # Native acquisition consumed a token, but this refusal sent
                # nothing. Return that exact unused reservation synchronously.
                self.bucket.remaining += 1
                tokens = self.bucket.remaining - self.bucket.outgoing
                if tokens > 0 and self.bucket._pending_requests:
                    self.bucket._wake(tokens)
            return False
        # Native zero-delay refresh yields once, then resets and wakes waiters.
        # Do not pre-empt that bookkeeping with an already-expired new timer.
        if self.bucket.reset_after == 0:
            return await self.bucket.__aexit__(exc_type, exc, traceback)
        try:
            # A new guard uses the SAME absolute deadline, not a fresh budget.
            # Pinned native exit releases outgoing before its first await;
            # cancellation of its refresh releases its own sleeping lock.
            deadline = self.deadline
            if isinstance(exc, asyncio.CancelledError):
                # User cancellation must not wait out even the remaining budget.
                deadline = min(deadline, asyncio.get_running_loop().time())
            async with asyncio.timeout_at(deadline):
                return await self.bucket.__aexit__(exc_type, exc, traceback)
        except TimeoutError:
            if isinstance(exc, asyncio.CancelledError):
                return False  # Preserve the original cancellation and its args.
            raise


class CreationEvidence:
    """Caller-owned, in-memory response syntax evidence; not dispatch approval."""
    __slots__ = ('_known_thread_id', '_claimed')

    def __init__(self):
        self._known_thread_id = None
        self._claimed = False

    @property
    def known_thread_id(self):
        return self._known_thread_id


class ThreadPort:
    def __init__(self, native):
        self.native = native
        self.state = getattr(native, '_connection', None)
        self.http = getattr(native, 'http', None)
        self.creation_failure = None
        self._lock = asyncio.Lock()
        self._global_retry_at = 0.0
        self._global_release = None
        self._server_floors = {}

    def _server_cooldown(self, bucket, operation):
        delay = self._server_floors.get(bucket, 0) - asyncio.get_running_loop().time()
        if delay > 0:
            raise SafeCooldown('route_cooldown', operation=operation,
                               scope='route', retry_after=delay) from None
        self._server_floors.pop(bucket, None)

    def presentation_ready(self, parent_id, *, card_id=None):
        """Read native admission state before card I/O; never reserve or send."""
        if (not _snowflake(parent_id) or (card_id is not None and not _snowflake(card_id))
                or not isinstance(self.http, discord.http.HTTPClient)):
            raise ClosedTransport('invalid_input') from None
        try:
            self._global_cooldown('card')
            # New cards also need a readback on the parent GET-message bucket.
            # Its native major parameter is the parent, not the unknown card ID.
            for method in (('POST', 'GET') if card_id is None else ('GET', 'PATCH')):
                route = discord.http.Route(method,
                    '/channels/{channel_id}/messages' if method == 'POST'
                    else '/channels/{channel_id}/messages/{message_id}',
                    channel_id=parent_id, message_id=card_id or '1')
                key = self.http._bucket_hashes.get(route.key, route.key)
                bucket = self.http.get_ratelimit(f'{key}:{route.major_parameters}')
                self._server_cooldown(bucket, 'card')
                if bucket.remaining <= 0 and not bucket.is_expired():
                    return False
        except SafeCooldown:
            return False
        return True

    @asynccontextmanager
    async def presentation_slot(self, parent_id, *, card_id=None):
        # A plugin readback/creation response must not install a new native
        # cooldown halfway through a card send/edit and its verification.
        async with self._lock:
            yield self.presentation_ready(parent_id, card_id=card_id)

    def _global_cooldown(self, operation):
        remaining = self._global_retry_at - asyncio.get_running_loop().time()
        if remaining > 0 or not self.http._global_over.is_set():
            raise SafeCooldown('global_cooldown', operation=operation,
                               scope='global', retry_after=remaining if remaining > 0 else None) from None

    @staticmethod
    def _route_cooldown(bucket, operation, retry_after=None):
        remaining = (bucket.expires or 0) - asyncio.get_running_loop().time()
        delay = max(remaining, retry_after or 0)
        if not math.isfinite(delay) or not 0 < delay <= 2**31:
            raise ClosedTransport('route_cooldown') from None
        raise SafeCooldown('route_cooldown', operation=operation, scope='route', retry_after=delay) from None

    async def readback(self, guild_id, parent_id, card_id, *, thread_id=None, timeout=15):
        if not (all(_snowflake(v) for v in (guild_id, parent_id, card_id))
                and (thread_id is None or _snowflake(thread_id))
                and type(timeout) in (int, float) and 0 < timeout <= 15
                and math.isfinite(timeout)):
            raise ClosedTransport('invalid_input') from None
        if discord.__version__ != '2.7.1' or aiohttp.__version__ != '3.14.3':
            raise ClosedTransport('unsupported_native') from None
        guild, parent, member, user = self._readback_cache(int(guild_id), int(parent_id))
        retained = self.native, self.state, self.http
        def check_cache():
            current = self._readback_cache(int(guild_id), int(parent_id))
            if any(a is not b for a, b in zip(current, (guild, parent, member, user))) or any(
                    a is not b for a, b in zip((self.native, self.state, self.http), retained)):
                raise ClosedTransport('unavailable') from None
        deadline = asyncio.get_running_loop().time() + timeout
        try:
            async with asyncio.timeout_at(deadline), self._lock:
                check_cache()
                data = await self._request_once('card', parent_id=parent_id,
                                                card_id=card_id, deadline=deadline)
                check_cache()
                self._readback_scope(data, id=card_id, channel_id=parent_id)
                try:
                    message = self.state.create_message(channel=parent, data=data)
                except Exception:
                    raise ClosedTransport('invalid_response') from None
                thread = None
                if thread_id is not None:
                    thread_data = await self._request_once('thread', thread_id=thread_id, deadline=deadline)
                    check_cache()
                    self._readback_scope(thread_data, id=thread_id, guild_id=guild_id, parent_id=parent_id)
                    if type(thread_data.get('type')) is not int or thread_data['type'] not in (10, 11, 12):
                        raise ClosedTransport('invalid_response') from None
                    try:
                        thread = discord.Thread(guild=guild, state=self.state, data=thread_data)
                    except Exception:
                        raise ClosedTransport('invalid_response') from None
                return message, parent, thread, member
        except TimeoutError:
            raise ClosedTransport('timeout') from None

    @staticmethod
    def _readback_scope(data, **expected):
        if type(data) is not dict or any(
                type(data.get(key)) is not str or not _snowflake(data[key])
                or int(data[key]) != int(value) for key, value in expected.items()):
            raise ClosedTransport('invalid_response') from None

    def _readback_cache(self, guild_id, parent_id):
        try:
            native, state = self.native, self.state
            user = native.user
            if not (isinstance(native, discord.Client)
                    and isinstance(state, discord.state.ConnectionState)
                    and native._connection is state and state.http is self.http
                    and native.http is self.http and isinstance(self.http, discord.http.HTTPClient)
                    and isinstance(user, discord.ClientUser) and state.user is user
                    and user._state is state and user.bot and _snowflake(user.id)):
                raise ValueError
            guild = native.get_guild(guild_id)
            if not (isinstance(guild, discord.Guild) and guild.id == guild_id
                    and not guild.unavailable and guild._state is state):
                raise ValueError
            parent = guild.get_channel(parent_id)
            member = guild.get_member(user.id)
            if not (isinstance(parent, discord.TextChannel) and parent.id == parent_id
                    and parent.guild is guild and parent._state is state
                    and isinstance(member, discord.Member) and member.id == user.id
                    and member.bot and member.guild is guild and member._state is state):
                raise ValueError
            return guild, parent, member, user
        except Exception:
            raise ClosedTransport('unavailable') from None

    async def create_once(self, parent_id, card_id, *, name, timeout=15, evidence=None, authorize=None):
        if authorize is not None and not callable(authorize):
            raise ClosedTransport('invalid_input') from None
        if evidence is not None:
            if type(evidence) is not CreationEvidence or evidence._claimed:
                raise ClosedTransport('invalid_input') from None
            evidence._claimed = True
        if self.creation_failure:
            raise ClosedTransport('http_429', message=self.creation_failure) from None
        if discord.__version__ != '2.7.1' or aiohttp.__version__ != '3.14.3':
            raise ClosedTransport('unsupported_native') from None
        if not (_snowflake(parent_id) and _snowflake(card_id)
                and isinstance(name, str) and 1 <= len(name) <= 100
                and name.strip() and not any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in name)
                and type(timeout) in (int, float) and 0 < timeout <= 15
                and math.isfinite(timeout)):
            raise ClosedTransport('invalid_input') from None
        deadline = asyncio.get_running_loop().time() + timeout
        try:
            async with asyncio.timeout_at(deadline), self._lock:
                if self.creation_failure:
                    raise ClosedTransport('http_429', message=self.creation_failure) from None
                return await self._request_once(
                    'create', parent_id=parent_id, card_id=card_id, name=name,
                    deadline=deadline, evidence=evidence, authorize=authorize)
        except TimeoutError:
            raise ClosedTransport('timeout', uncertain=True) from None
        except (aiohttp.ClientError, ConnectionError):
            raise ClosedTransport('transport_error', uncertain=True) from None

    async def _request_once(self, operation, *, parent_id=None, card_id=None, thread_id=None, name=None, deadline, evidence=None, authorize=None):
        """Closed operations; caller owns admission, lock and absolute timeout.

        GET may retry inside aiohttp; this helper has no application retry loop.
        Outer deadline cancellation is preserved for caller classification.
        """
        if operation not in ('create', 'card', 'thread'):
            raise ClosedTransport('invalid_input') from None
        if operation != 'create' and (evidence is not None or not (
                _snowflake(thread_id) if operation == 'thread'
                else _snowflake(parent_id) and _snowflake(card_id))):
            raise ClosedTransport('invalid_input') from None
        http = self.http
        session = getattr(http, '_HTTPClient__session', None)
        if not (isinstance(http, discord.http.HTTPClient)
                and isinstance(session, aiohttp.ClientSession) and not session.closed
                and callable(getattr(session, 'request', None))
                and isinstance(getattr(http, '_bucket_hashes', None), dict)
                and isinstance(getattr(http, '_buckets', None), dict)
                and isinstance(getattr(http, '_global_over', None), asyncio.Event)
                and callable(getattr(http, 'get_ratelimit', None))
                and type(getattr(http, 'use_clock', None)) is bool
                and isinstance(getattr(http, 'token', None), str) and http.token
                and (getattr(http, 'proxy', False) is None or isinstance(http.proxy, str))
                and (getattr(http, 'proxy_auth', False) is None or isinstance(http.proxy_auth, aiohttp.BasicAuth))):
            raise ClosedTransport('unsupported_native') from None
        route = discord.http.Route(
            'POST' if operation == 'create' else 'GET',
            '/channels/{channel_id}/messages/{message_id}/threads' if operation == 'create'
            else '/channels/{channel_id}/messages/{message_id}' if operation == 'card'
            else '/channels/{channel_id}',
            channel_id=thread_id if operation == 'thread' else parent_id, message_id=card_id)
        body = {'json': {'name': name, 'auto_archive_duration': 1440}} if operation == 'create' else {}
        self._global_cooldown(operation)
        key = self.http._bucket_hashes.get(route.key, route.key)
        bucket = self.http.get_ratelimit(f'{key}:{route.major_parameters}')
        self._server_cooldown(bucket, operation)
        if bucket.remaining <= 0 and not bucket.is_expired():
            self._route_cooldown(bucket, operation)
        try:
            async with AsyncExitStack() as stack:
                try:
                    await stack.enter_async_context(_Reservation(bucket, deadline))
                except discord.RateLimited as exc:
                    # Only native acquisition refusal is definitely prewire.
                    self._route_cooldown(bucket, operation, exc.retry_after)
                self._global_cooldown(operation)
                self._server_cooldown(bucket, operation)
                if authorize is not None and authorize() is not True:
                    raise ClosedTransport('authority_revoked') from None
                async with self.http._HTTPClient__session.request(
                    route.method, route.url, headers={'Authorization': 'Bot ' + self.http.token},
                    proxy=self.http.proxy, proxy_auth=self.http.proxy_auth,
                    **body, allow_redirects=False,
                ) as response:
                    try:
                        data = await response.json()
                    except (ValueError, aiohttp.ContentTypeError):
                        raise ClosedTransport('invalid_response', uncertain=operation == 'create') from None
                    identity = data.get('id') if type(data) is dict else None
                    valid_identity = type(identity) is str and _snowflake(identity)
                    if evidence is not None and 200 <= response.status < 300 and valid_identity:
                        evidence._known_thread_id = identity
                    if response.status == 429:
                        delay = data.get('retry_after') if type(data) is dict else None
                        scope = response.headers.get('X-Ratelimit-Scope', 'user')
                        valid = (response.headers.get('Via') and type(data) is dict
                            and type(delay) in (int, float) and math.isfinite(delay)
                            and 0 < delay <= 2**31 and type(data.get('global', False)) is bool
                            and scope in ('user', 'shared', 'global')
                            and (scope != 'global' or data.get('global') is True)
                            and (data.get('global') is not True or scope != 'shared'))
                        if valid:
                            scope = 'global' if data.get('global') is True else 'shared' if scope == 'shared' else 'route'
                            response_hash = response.headers.get('X-Ratelimit-Bucket')
                            if response_hash is not None:
                                mapped = f'{response_hash}:{route.major_parameters}'
                                existing = http._buckets.get(mapped)
                                if existing is not None and existing is not bucket:
                                    if scope == 'global':
                                        self.creation_failure = 'Ring thread creation stopped after global HTTP 429; manual recovery required'
                                    raise ClosedTransport('bucket_collision', uncertain=operation == 'create') from None
                                http._bucket_hashes[route.key] = response_hash
                                http._buckets[mapped] = bucket
                            now = asyncio.get_running_loop().time()
                            bucket.expires = max(bucket.expires or 0, now + delay)
                            bucket.reset_after = bucket.expires - now
                            bucket.remaining = 0
                            self._server_floors = {b: due for b, due in self._server_floors.items() if due > now}
                            self._server_floors[bucket] = max(self._server_floors.get(bucket, 0), bucket.expires)
                            if scope == 'global':
                                self._global_retry_at = max(self._global_retry_at, bucket.expires)
                                http._global_over.clear()
                                if self._global_release is not None:
                                    self._global_release.cancel()
                                self._global_release = asyncio.get_running_loop().call_at(
                                    self._global_retry_at, http._global_over.set)
                            raise SafeCooldown('http_429_' + scope, operation=operation,
                                scope=scope, retry_after=bucket.reset_after) from None
                        if response.headers.get('Via') and isinstance(data, dict) and data.get('global') is True:
                            self.creation_failure = 'Ring thread creation stopped after global HTTP 429; manual recovery required'
                    if response.status != 429 and 'X-Ratelimit-Remaining' in response.headers:
                        try:
                            for field, default in (('X-Ratelimit-Limit', 1), ('X-Ratelimit-Remaining', 0)):
                                if int(response.headers.get(field, default)) < 0:
                                    raise ValueError
                            reset_after = response.headers.get('X-Ratelimit-Reset-After')
                            if self.http.use_clock or not reset_after:
                                import datetime
                                now = datetime.datetime.now(datetime.timezone.utc)
                                reset = datetime.datetime.fromtimestamp(
                                    float(response.headers['X-Ratelimit-Reset']), datetime.timezone.utc)
                                delay = (reset - now).total_seconds()
                            else:
                                delay = float(reset_after)
                            if not math.isfinite(delay) or delay < 0:
                                raise ValueError
                        except (ValueError, TypeError, OverflowError, KeyError, OSError):
                            raise ClosedTransport('accounting_error', uncertain=operation == 'create') from None
                    response_hash = response.headers.get('X-Ratelimit-Bucket')
                    if response_hash is not None:
                        mapped = f'{response_hash}:{route.major_parameters}'
                        existing = self.http._buckets.get(mapped)
                        if existing is not None and existing is not bucket:
                            raise ClosedTransport('bucket_collision', uncertain=operation == 'create') from None
                        self.http._bucket_hashes[route.key] = response_hash
                        self.http._buckets[mapped] = bucket
                    if 'X-Ratelimit-Remaining' in response.headers and response.status != 429:
                        try:
                            bucket.update(response, use_clock=self.http.use_clock)
                        except (ValueError, TypeError, OverflowError):
                            raise ClosedTransport('accounting_error', uncertain=operation == 'create') from None
                    if not 200 <= response.status < 300:
                        raise ClosedTransport(f'http_{response.status}',
                                              uncertain=operation == 'create' and not 400 <= response.status < 500) from None
                    if not valid_identity:
                        raise ClosedTransport('invalid_response', uncertain=operation == 'create') from None
                    return data
        except TimeoutError:
            if operation == 'create':
                raise
            raise ClosedTransport('timeout') from None
        except (aiohttp.ClientError, ConnectionError):
            if operation == 'create':
                raise
            raise ClosedTransport('transport_error') from None

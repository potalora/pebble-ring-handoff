"""Instance-local admission before native lane reservation (no state policy)."""
import asyncio
import inspect

NOTICE_TIMEOUT_SECONDS = 1.0


def _decision(callback, event):
    result = callback(event)
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        elif hasattr(result, 'cancel'):
            result.cancel()
        return None
    return result if type(result) is bool else None


def _validate(callback, *, asynchronous, exact=False):
    if not callable(callback):
        raise TypeError('admission callback must be callable')
    implementation = callback if inspect.isroutine(callback) else callback.__call__
    if inspect.iscoroutinefunction(implementation) is not asynchronous or inspect.isasyncgenfunction(implementation):
        raise TypeError('admission callback async contract mismatch')
    try:
        signature = inspect.signature(callback)
        signature.bind(object())
    except (TypeError, ValueError) as exc:
        raise TypeError('admission callback must accept one event') from exc
    if exact:
        parameters = list(signature.parameters.values())
        if (len(parameters) != 1 or parameters[0].name != 'event'
                or parameters[0].kind is not inspect.Parameter.POSITIONAL_OR_KEYWORD
                or parameters[0].default is not inspect.Parameter.empty):
            raise TypeError('native handle_message must be async (event)')


class AdmissionGuard:
    def __init__(self, adapter, *, is_ring_scope, allow, notify, route=None):
        self.adapter = adapter
        self.is_ring_scope = is_ring_scope
        self.allow = allow
        self.notify = notify
        self.route = route
        self._closed = False
        self._wrapper = None
        self._original = None

    def install(self):
        if self._closed:
            return
        if self._wrapper is not None:
            if self.adapter.handle_message is not self._wrapper:
                raise RuntimeError('admission guard ownership lost')
            return
        if getattr(self.adapter, '_ring_admission_guard', None) is not None:
            raise RuntimeError('adapter already has an admission owner')
        original = self.adapter.handle_message
        _validate(original, asynchronous=True, exact=True)
        _validate(self.is_ring_scope, asynchronous=False)
        _validate(self.allow, asynchronous=False)
        _validate(self.notify, asynchronous=True)
        if self.route is not None:
            _validate(self.route, asynchronous=True)

        async def handle_message(event):
            try:
                scope = _decision(self.is_ring_scope, event)
                if scope is True and not self._closed and self.route is not None:
                    event = await self.route(event)
                    if _decision(self.is_ring_scope, event) is not True:
                        raise PermissionError('Ring routing left its scope')
                admitted = scope is False or (scope is True and not self._closed
                                              and _decision(self.allow, event) is True)
            except Exception:
                admitted = False
            if not admitted:
                try:
                    async with asyncio.timeout(NOTICE_TIMEOUT_SECONDS):
                        await self.notify(event)
                except Exception:
                    pass  # Delivery failure never changes admission; no raw logging.
                return None
            return await original(event)

        self._original = original
        self._wrapper = handle_message
        self.adapter._ring_admission_guard = self
        self.adapter.handle_message = handle_message

    async def close(self):
        self._closed = True
        if self._wrapper is not None and self.adapter.handle_message is self._wrapper:
            self.adapter.handle_message = self._original
        if getattr(self.adapter, '_ring_admission_guard', None) is self:
            del self.adapter._ring_admission_guard

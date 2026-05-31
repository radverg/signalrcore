import asyncio
import inspect
import time
from typing import Awaitable, Any, List, Callable, Optional
from ...hub.base_hub_connection import BaseHubConnection
from ...transport.base_transport import TransportState
from ...messages.completion_message import CompletionMessage


class AIOBaseHubConnection(BaseHubConnection):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loop = None  # captured in start()

    async def wait_until_state(
            self,
            state: TransportState,
            timeout: float = None) -> Awaitable:
        t0 = time.time()
        while self.transport is None or self.transport.state != state:
            await asyncio.sleep(0.1)
            if timeout is not None and t0 + timeout < time.time():  # pragma: no cover # noqa E501
                raise TimeoutError()
        self.logger.info(
            f"Time elapsed until state change {time.time() - t0}s")

    async def start(self) -> Awaitable:
        """Starts the connection and waits until the connection
        is ready.

        Returns:
            bool: True if connection stars successfully, False
            if connection cant start or is already connected
        """
        self._loop = asyncio.get_event_loop()
        t1 = asyncio.to_thread(super().start)
        t2 = self.wait_until_state(TransportState.connected)

        result, _ = await asyncio.gather(t1, t2)

        return result

    async def stop(self) -> Awaitable:
        """Stops the connection and waits until the connection
        is closed

        Returns:
            None
        """
        t1 = asyncio.to_thread(super().stop)
        t2 = self.wait_until_state(TransportState.disconnected)
        result, _ = await asyncio.gather(t1, t2)
        return result

    async def send(
            self,
            method: str,
            arguments: List[Any],
            on_invocation: Optional[Callable[[List[CompletionMessage]], Awaitable[None]]] = None,  # noqa: E501
            invocation_id: str = None) -> Awaitable:
        """invokes a server function

        Args:
            method (string): Method name
            arguments (list|Subject): Method parameters
            on_invocation (function, optional): On invocation send callback
                will be raised on send server function ends. Defaults to None.
            invocation_id (string, optional): Override invocation ID.
                Exceptions thrown by the hub will use this ID,
                making it easier to handle with the on_error call.

        Raises:
            HubConnectionError: If hub is not ready to send
            TypeError: If arguments are invalid list or Subject
        """
        result = await asyncio.to_thread(
            super().invoke,
            method,
            arguments,
            on_invocation,
            invocation_id
        )
        return result

    async def invoke(
            self,
            method: str,
            arguments: List[Any],
            on_invocation: Optional[Callable[[List[CompletionMessage]], Awaitable[None]]] = None,  # noqa: E501
            invocation_id: str = None):
        """invokes a server function

        Args:
            method (string): Method name
            arguments (list|Subject): Method parameters
            on_invocation (function, optional): On invocation send callback
                will be raised on send server function ends. Defaults to None.
            invocation_id (string, optional): Override invocation ID.
                Exceptions thrown by the hub will use this ID,
                making it easier to handle with the on_error call.

        Raises:
            HubConnectionError: If hub is not ready to send
            TypeError: If arguments are invalid list or Subject
        """
        result = await asyncio.to_thread(
            super().invoke,
            method,
            arguments,
            on_invocation,
            invocation_id
        )
        return result

    def on(
            self,
            event: str,
            callback_function: Callable[[List[Any]], None])\
            -> None:
        """Register a callback on the specified event
        Args:
            event (string):  Event name
            callback_function (Function): callback function,
                arguments will be bound
        """
        return super().on(event, callback_function)

    def on_with_result(self, event: str, callback: Callable) -> None:
        """Register a callback for server-to-client invocations that expect
        a result back (client results, introduced in SignalR .NET 7).

        The callback may be either a regular function or an async coroutine.
        It receives the arguments list and must return (or resolve with) the
        result value.  Exceptions are forwarded to the server as error strings.

        connection.on_with_result("GetMessage", lambda args: "Hello!")

        async def get_message(args):
            await asyncio.sleep(0)
            return "Hello async!"
        connection.on_with_result("GetMessage", get_message)

        Args:
            event (string): Event name
            callback (Callable): sync or async callback that accepts the
                arguments list and returns a result value
        """
        if inspect.iscoroutinefunction(callback):
            loop = self._loop

            def sync_wrapper(arguments):
                if loop is None or not loop.is_running():
                    # Fallback: run a new event loop (e.g. before start())
                    return asyncio.run(callback(arguments))
                future = asyncio.run_coroutine_threadsafe(
                    callback(arguments), loop)
                return future.result()

            super().on_with_result(event, sync_wrapper)
        else:
            super().on_with_result(event, callback)


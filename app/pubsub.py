"""
Pub/sub command implementations: PUBLISH, SUBSCRIBE, UNSUBSCRIBE.
"""

import socket
from typing import List

from . import state


def cmd_publish(parts: List[str]) -> str:
    """
    Handle the PUBLISH command.

    :param parts: Parsed RESP command tokens.
    :returns: RESP-encoded integer count of subscribers that received the message.
    """
    channel, message = parts[1], parts[2]
    count = 0
    with state.subscriptions_lock:
        if channel in state.channel_subscribers:
            count = len(state.channel_subscribers[channel])
            msg = (
                f"*3\r\n$7\r\nmessage\r\n"
                f"${len(channel)}\r\n{channel}\r\n"
                f"${len(message)}\r\n{message}\r\n"
            ).encode("utf-8")
            for sub in list(state.channel_subscribers[channel]):
                try:
                    sub.send(msg)
                except (socket.error, OSError):
                    pass
    return f":{count}\r\n"


def cmd_subscribe(client: socket.socket, parts: List[str]) -> None:
    """
    Register the client for the requested channels and send confirmation messages.

    :param client: Connected client socket.
    :param parts: Parsed RESP command tokens (channels follow the command name).
    """
    with state.subscriptions_lock:
        if client not in state.client_subscriptions:
            state.client_subscriptions[client] = set()
        for channel in parts[1:]:
            state.channel_subscribers.setdefault(channel, set()).add(client)
            state.client_subscriptions[client].add(channel)
            count = len(state.client_subscriptions[client])
            client.send(
                f"*3\r\n$9\r\nsubscribe\r\n"
                f"${len(channel)}\r\n{channel}\r\n:{count}\r\n".encode("utf-8")
            )


def cmd_unsubscribe(client: socket.socket, parts: List[str]) -> bool:
    """
    Unregister the client from channels.

    :param client: Connected client socket.
    :param parts: Parsed RESP command tokens (channels follow the command name).
    :returns: ``True`` if the client remains subscribed to at least one channel.
    """
    with state.subscriptions_lock:
        for channel in parts[1:]:
            if channel in state.channel_subscribers:
                state.channel_subscribers[channel].discard(client)
                if not state.channel_subscribers[channel]:
                    del state.channel_subscribers[channel]
            if client in state.client_subscriptions:
                state.client_subscriptions[client].discard(channel)
            count = len(state.client_subscriptions.get(client, set()))
            client.send(
                f"*3\r\n$11\r\nunsubscribe\r\n"
                f"${len(channel)}\r\n{channel}\r\n:{count}\r\n".encode("utf-8")
            )
        still_subscribed = bool(state.client_subscriptions.get(client))
        if not still_subscribed and client in state.client_subscriptions:
            del state.client_subscriptions[client]
    return still_subscribed

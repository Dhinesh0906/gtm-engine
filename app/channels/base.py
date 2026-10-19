"""Abstract base class for outbound/inbound channels (email, X, LinkedIn, etc.)."""

from abc import ABC, abstractmethod


class Channel(ABC):
    """Abstract interface for a communication channel."""

    @abstractmethod
    def identity(self) -> str:
        """Return a stable identifier for this channel, e.g. 'email'."""
        raise NotImplementedError

    @abstractmethod
    def can_reach(self, contact_point) -> bool:
        """Return True if this channel can deliver to the given contact point."""
        raise NotImplementedError

    @abstractmethod
    def send(self, to_value: str, subject: str, body: str) -> str:
        """Send a message and return a provider-specific message id."""
        raise NotImplementedError

    @abstractmethod
    def poll_inbound(self) -> list:
        """Poll for inbound messages and return a list of message dicts."""
        raise NotImplementedError

    @abstractmethod
    def rate_limit(self) -> dict:
        """Return the current rate-limit state for this channel."""
        raise NotImplementedError

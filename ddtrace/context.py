from typing import Optional

from .constants import ORIGIN_KEY
from .constants import SAMPLING_PRIORITY_KEY
from .internal.logger import get_logger
from .span import Span


log = get_logger(__name__)


class Context(object):
    def __init__(self, trace_id=None, span_id=None, sampling_priority=None, dd_origin=None):
        """
        :param int trace_id: trace_id of parent span
        :param int span_id: span_id of parent span
        """
        self._active_span = None
        self._trace_root_span_id = None  # root span might be a remote span.
        self._local_root_span = None
        self._trace_id = trace_id
        self._active_span_id = span_id
        self._sampling_priority = sampling_priority
        self._dd_origin = dd_origin

    @property
    def sampling_priority(self):
        return self._sampling_priority

    @sampling_priority.setter
    def sampling_priority(self, value):
        if self._local_root_span:
            self._local_root_span.metrics[SAMPLING_PRIORITY_KEY] = value
        self._sampling_priority = value

    @property
    def dd_origin(self):
        return self._dd_origin

    @dd_origin.setter
    def dd_origin(self, value):
        if self._local_root_span:
            self._local_root_span.meta[ORIGIN_KEY] = value
        self._dd_origin = value

    @property
    def trace_id(self):
        """Return context trace_id."""
        return self._trace_id

    @property
    def span_id(self):
        """Return active context span_id."""
        return self._active_span_id

    def clone(self):
        new_ctx = Context(
            trace_id=self._trace_id,
            span_id=self._active_span_id,
            sampling_priority=self.sampling_priority,
            dd_origin=self.dd_origin,
        )
        new_ctx._active_span_id = self._active_span_id
        return new_ctx

    def get_current_root_span(self):
        # type: () -> Optional[Span]
        """
        Return the root span of the context or None if it does not exist.
        """
        return self._local_root_span

    def get_current_span(self):
        # type: () -> Optional[Span]
        """
        Return the last active span that corresponds to the last inserted
        item in the trace list. This cannot be considered as the current active
        span in asynchronous environments, because some spans can be closed
        earlier while child spans still need to finish their traced execution.
        """
        return self._active_span

    def _set_current_span(self, span):
        self._active_span = span
        if span:
            self._trace_id = span.trace_id
            self._active_span_id = span.span_id
        else:
            self._active_span_id = None

    def add_span(self, span):
        # type: (Span) -> None
        """Activate span in the context."""
        if not span._parent:
            self._local_root_span = span
            if self.dd_origin is not None:
                span.meta[ORIGIN_KEY] = self.dd_origin
            if self.sampling_priority is not None:
                span.metrics[SAMPLING_PRIORITY_KEY] = self.sampling_priority
        self._set_current_span(span)
        span._context = self

    def close_span(self, span):
        if span == self._local_root_span:
            self._local_root_span = None
            # Also reset the root span id (only one trace can inherit from a distributed trace)
            self._trace_root_span_id = None
            self._trace_id = None
        self._set_current_span(span._parent)

"""硬件单调计数器（Occlum/SGX 环境下可替换为真实 MC）。"""

from __future__ import annotations

import threading


class InMemoryMonotonicCounter:
    def __init__(self) -> None:
        self._value = 0
        self._lock = threading.Lock()

    def increment(self, namespace: str = "") -> int:
        del namespace
        with self._lock:
            self._value += 1
            return self._value

    def current(self, namespace: str = "") -> int:
        del namespace
        with self._lock:
            return self._value

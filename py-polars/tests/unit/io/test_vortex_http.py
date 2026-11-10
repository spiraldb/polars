"""Test native Vortex reads through the Polars HTTP byte source."""

from __future__ import annotations

import contextlib
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any

import pytest

import polars as pl
from polars.testing import assert_frame_equal

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


def _native_vortex_available() -> bool:
    from polars import _plr

    return hasattr(_plr.PyLazyFrame, "new_from_vortex")


pytestmark = pytest.mark.skipif(
    not _native_vortex_available(),
    reason="polars was not built with the 'vortex' feature",
)


@contextlib.contextmanager
def _range_server(data: bytes) -> Iterator[tuple[str, list[tuple[int, int]]]]:
    ranges: list[tuple[int, int]] = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(5)

        def _serve(self, *, body: bool) -> None:
            if self.path != "/fixture.vortex":
                self.send_error(404)
                return
            start, end = 0, len(data)
            range_header = self.headers.get("Range")
            if range_header is not None:
                if not range_header.startswith("bytes=") or "," in range_header:
                    self.send_error(400)
                    return
                first, last = range_header.removeprefix("bytes=").split("-", 1)
                if first:
                    start = int(first)
                    end = min(int(last) + 1, len(data)) if last else len(data)
                else:
                    start = max(len(data) - int(last), 0)
                if not 0 <= start < end <= len(data):
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{len(data)}")
                    self.send_header("Content-Length", "0")
                    self.send_header("Connection", "close")
                    self.end_headers()
                    return
                self.send_response(206)
                self.send_header(
                    "Content-Range", f"bytes {start}-{end - 1}/{len(data)}"
                )
            else:
                self.send_response(200)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(end - start))
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("ETag", '"vortex-fixture"')
            self.send_header("Connection", "close")
            self.end_headers()
            if body:
                try:
                    self.wfile.write(data[start:end])
                except (BrokenPipeError, ConnectionResetError):
                    return
                with lock:
                    ranges.append((start, end))

        def do_HEAD(self) -> None:
            self._serve(body=False)

        def do_GET(self) -> None:
            self._serve(body=True)

        def log_message(self, format: str, *args: Any) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        server.daemon_threads = True
        server.block_on_close = False
        thread = threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/fixture.vortex", ranges
        finally:
            server.shutdown()
            thread.join(timeout=5)
            assert not thread.is_alive(), "HTTP fixture server did not stop"


def test_native_vortex_http_projection_and_filter(tmp_path: Path) -> None:
    vx = pytest.importorskip("vortex")
    pa = pytest.importorskip("pyarrow")
    n_rows = 16_384
    item_size = 512
    entropy = random.Random(0).randbytes(n_rows * item_size)
    payload = [
        entropy[offset : offset + item_size]
        for offset in range(0, len(entropy), item_size)
    ]
    path = tmp_path / "fixture.vortex"
    vx.io.write(
        pa.table(
            {
                "id": pa.array(range(n_rows), type=pa.int64()),
                "unused": pa.array(payload, type=pa.binary()),
            }
        ),
        str(path),
    )
    data = path.read_bytes()
    assert len(data) > 4 * 1024 * 1024

    with _range_server(data) as (url, ranges):
        result = (
            pl.scan_vortex(url, cache_mode="off", initial_read_size=4096)
            .filter(pl.col("id") >= n_rows - 8)
            .select("id")
            .collect()
        )

    assert_frame_equal(result, pl.DataFrame({"id": range(n_rows - 8, n_rows)}))
    assert ranges, "The HTTP source did not receive a data request"
    assert all(0 <= start < end <= len(data) for start, end in ranges)
    # The large unused column must not require a full download.
    assert sum(end - start for start, end in ranges) < len(data) // 2

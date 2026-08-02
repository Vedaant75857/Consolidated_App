from __future__ import annotations

import io
import zipfile

import pytest

from backend.ingestion import ArchiveLimits, iter_archive_members
import backend.ingestion.archives as archives


def _many_members(count: int) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        for index in range(count):
            # Keep each member below the default bomb ratio while making the
            # central-directory traversal cost observable.
            archive.writestr(f"rows-{index:04d}.csv", f"value-{index}\n".encode())
    return out.getvalue()


@pytest.mark.parametrize("count", [100, 500, 1000])
def test_archive_member_traversal_scales_linearly(monkeypatch: pytest.MonkeyPatch, count: int) -> None:
    payload = _many_members(count)
    original = archives.zipfile.ZipFile
    stats = {"construct": 0, "infolist": 0, "open": 0}

    class CountingZipFile(original):
        def __init__(self, *args: object, **kwargs: object) -> None:
            stats["construct"] += 1
            super().__init__(*args, **kwargs)

        def infolist(self):  # type: ignore[no-untyped-def]
            stats["infolist"] += 1
            return super().infolist()

        def open(self, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
            stats["open"] += 1
            return super().open(*args, **kwargs)

    monkeypatch.setattr(archives.zipfile, "ZipFile", CountingZipFile)
    members = list(iter_archive_members(payload, ArchiveLimits(chunk_size=32)))
    try:
        assert len(members) == count
        assert stats == {"construct": 1, "infolist": 1, "open": count}
        assert members[0].read_bytes().startswith(b"value-0")
        assert members[-1].read_bytes().startswith(f"value-{count - 1}".encode())
    finally:
        for member in members:
            member.close()

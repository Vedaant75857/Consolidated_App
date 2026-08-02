"""Stable diagnostic helpers for adapters and future readers."""
from __future__ import annotations

from .models import Issue, IssueCode, IssueSeverity

FATAL_CODES = frozenset({
    IssueCode.UNSUPPORTED_FORMAT,
    IssueCode.DECODING_ERROR,
    IssueCode.INVALID_ARCHIVE,
    IssueCode.ARCHIVE_LIMIT,
    IssueCode.ARCHIVE_PATH_TRAVERSAL,
    IssueCode.ARCHIVE_ENCRYPTED,
    IssueCode.ARCHIVE_CORRUPT_ENTRY,
})


def issue(code: IssueCode, message: str, *, severity: IssueSeverity | None = None,
          source: str | None = None, location: str | None = None, details=None) -> Issue:
    severity = severity or (IssueSeverity.ERROR if code in FATAL_CODES else IssueSeverity.WARNING)
    return Issue(code, severity, message, source, location, details or {}, recoverable=code not in FATAL_CODES)

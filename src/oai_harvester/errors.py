class HarvesterError(Exception):
    """Base error for harvest pipeline."""


class OAIError(HarvesterError):
    """Raised when OAI-PMH endpoint returns an error response."""


class OAIProtocolError(OAIError):
    """Raised when OAI-PMH returned an error with a protocol code."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"OAI-PMH error ({code}): {message}")


class OAITransportError(HarvesterError):
    """Raised when network/protocol transport fails."""


class OAIParseError(HarvesterError):
    """Raised when OAI-PMH XML cannot be parsed."""


class OAINoRecords(HarvesterError):
    """Raised when endpoint responds with OAI-PMH noRecordsMatch."""

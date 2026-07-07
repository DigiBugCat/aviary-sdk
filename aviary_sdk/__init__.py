"""aviary_sdk — a bird's faces for free (ARCHITECTURE.md §5.1).

    from aviary_sdk import Bird, Stream

    bird = Bird("heron", port=7331,
                streams=[Stream("inbox", "queue", keep_last=5000),
                         Stream("digests", "ledger")])
    bird.serve()   # /birdz, /mcp, /acc/*, REF stub, audit, ~/.aviary/heron/crop.db
"""
from .acc import Crop, Stream
from .bird import Bird

__all__ = ["Bird", "Crop", "Stream"]
__version__ = "0.1.1"

from .oneshot import MODELS, OneshotError, oneshot, oneshot_async  # noqa: E402,F401
from ._response import Response, ResponseUsage  # noqa: E402,F401
from .openai import AsyncOpenAI, OpenAI  # noqa: E402,F401

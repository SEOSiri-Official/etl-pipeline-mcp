
# =========================================================================
# PILLAR 2: STRICT PYDANTIC INPUT VALIDATION CONTRACTS
# =========================================================================
from pydantic import BaseModel, Field

class ValidatedToolRequest(BaseModel):
    client_id: str = Field(default="ANONYMOUS_CLIENT", min_length=1)
    timeout_seconds: int = Field(default=30, ge=1, le=300)
    strict_mode: bool = Field(default=True)


# =========================================================================
# PILLAR 4: RESILIENCE & CIRCUIT BREAKER ENGINE
# =========================================================================
import time
from typing import Callable, Any

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, recovery_time: float = 10.0):
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.state = 'CLOSED'

    def execute(self, func: Callable, fallback_func: Callable, *args, **kwargs) -> Any:
        now = time.time()
        if self.state == 'OPEN':
            if now - self.last_failure_time > self.recovery_time:
                self.state = 'HALF_OPEN'
            else:
                return fallback_func(*args, **kwargs)
        try:
            res = func(*args, **kwargs)
            self.failure_count = 0
            self.state = 'CLOSED'
            return res
        except Exception:
            self.failure_count += 1
            self.last_failure_time = now
            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'
            return fallback_func(*args, **kwargs)

resilience_circuit_breaker = CircuitBreaker()

from setuptools import setup, find_packages

setup(
    name="etl-pipeline-mcp",
    version="1.0.3",
    packages=find_packages(),
)

# =========================================================================
# PILLAR 1: DUAL TRANSPORT PARITY (stdio + SSE)
# =========================================================================
if __name__ == '__main__':
    import os, sys
    transport = os.getenv('MCP_TRANSPORT', 'stdio').lower()
    if '--sse' in sys.argv or transport == 'sse':
        mcp.run(transport='sse')
    else:
        mcp.run(transport='stdio')

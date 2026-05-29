"""
message.py — Schema del mensaje que viaja por los tópicos Kafka.

Cada consulta publicada en Kafka se serializa como JSON
  - query_id        (uuid4 unico, sobrevive a reintentos)
  - query_type      (Q1..Q5)
  - params          (dict con los parametros especificos de la consulta)
  - retry_count     (cuantas veces se ha reenviado a queries.retry)
  - created_at      (epoch seconds en UTC, del primer envio)
  - last_attempt_at (epoch seconds en UTC, del intento mas reciente)

El query_id NO se regenera al reintentar — se conserva para poder
trazar el ciclo de vida completo de una consulta (creacion -> N reintentos ->
exito o DLQ).
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, asdict, field


@dataclass
class QueryMessage:
    query_id: str
    query_type: str
    params: dict
    retry_count: int = 0
    created_at: float = field(default_factory=time.time)
    last_attempt_at: float = field(default_factory=time.time)

    #  Constructores 
    @classmethod
    def new(cls, query_type: str, params: dict) -> "QueryMessage":
        """Crea un mensaje nuevo (retry_count=0, timestamps al instante actual)."""
        now = time.time()
        return cls(
            query_id=str(uuid.uuid4()),
            query_type=query_type,
            params=dict(params),
            retry_count=0,
            created_at=now,
            last_attempt_at=now,
        )

    def with_retry(self) -> "QueryMessage":
        """
        Devuelve una nueva instancia con retry_count+1 y last_attempt_at
        actualizado al instante actual. Mantiene query_id y created_at para
        que el mensaje sea trazable a lo largo de sus reintentos.
        """
        return QueryMessage(
            query_id=self.query_id,
            query_type=self.query_type,
            params=self.params,
            retry_count=self.retry_count + 1,
            created_at=self.created_at,
            last_attempt_at=time.time(),
        )

    #  Serializacion 
    def to_json_bytes(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> "QueryMessage":
        return cls(**json.loads(raw.decode("utf-8")))

    #  Helpers 
    def age_seconds(self) -> float:
        """Tiempo transcurrido desde created_at."""
        return time.time() - self.created_at

"""
faults.py — Inyección de fallos configurable para los consumers (Tarea 2).

Modela la degradación o caída del "Generador de Respuestas": el motor que
resuelve las consultas cuando hay un cache MISS. La caída NO afecta a los
cache HIT — Redis sigue sirviendo lo que ya tiene almacenado. Esto refleja
el comportamiento real: aunque el backend de cómputo falle, la caché amortigua
parte del tráfico.

Modos (combinables, configurados por env vars en kafka_config):
  - Latencia artificial (ARTIFICIAL_LATENCY_MS): suma latencia a cada MISS.
  - Caída permanente (RESPONSES_DOWN): todo MISS falla durante el run completo.
  - Caída temporal (BACKEND_DOWN_START_S / BACKEND_DOWN_DURATION_S): el motor
    falla solo dentro de una ventana de tiempo desde el arranque del worker;
    fuera de ella funciona. Permite demostrar recuperación automática vía
    reintentos.
  - Fallo aleatorio (FAIL_RATE): cada MISS falla con probabilidad FAIL_RATE.

La inyección se realiza envolviendo el QueryEngine con FaultyEngine. Como
CacheService solo invoca engine.run() en un MISS, los HIT quedan exentos de
forma natural, sin tocar la lógica de la Tarea 1.
"""

import time
import random


class ResponseGeneratorDown(Exception):
    """El Generador de Respuestas no pudo resolver la consulta (fallo simulado)."""


class FaultInjector:
    """Decide y aplica los fallos según la configuración y el tiempo transcurrido."""

    def __init__(self, fail_rate: float = 0.0, responses_down: bool = False,
                 latency_ms: float = 0.0, down_start_s: float = 0.0,
                 down_duration_s: float = 0.0):
        self.fail_rate = fail_rate
        self.responses_down = responses_down
        self.latency_ms = latency_ms
        self.down_start_s = down_start_s
        self.down_duration_s = down_duration_s
        # Reloj de la ventana de caída temporal: se inicializa perezosamente
        # en la PRIMERA invocación de apply(), es decir, al primer cache MISS.
        # Así la ventana siempre cubre tiempo de procesamiento real y no se
        # "gasta" durante la carga del dataset o el join al grupo de Kafka.
        self._t0 = None

    def _in_down_window(self) -> bool:
        if self.down_duration_s <= 0 or self._t0 is None:
            return False
        elapsed = time.monotonic() - self._t0
        return self.down_start_s <= elapsed < (self.down_start_s + self.down_duration_s)

    def is_backend_down(self) -> bool:
        """True si el motor debe fallar ahora (caída permanente o ventana activa)."""
        return self.responses_down or self._in_down_window()

    def apply(self) -> None:
        """
        Aplica latencia artificial y, si corresponde, lanza ResponseGeneratorDown.
        Se invoca justo antes de resolver una consulta en el motor (solo MISS).
        """
        # Inicialización perezosa del reloj de la ventana de caída.
        if self._t0 is None and self.down_duration_s > 0:
            self._t0 = time.monotonic()
        if self.latency_ms > 0:
            time.sleep(self.latency_ms / 1000.0)
        if self.is_backend_down():
            raise ResponseGeneratorDown("Generador de Respuestas caído")
        if self.fail_rate > 0 and random.random() < self.fail_rate:
            raise ResponseGeneratorDown(f"fallo aleatorio (fail_rate={self.fail_rate})")

    @property
    def enabled(self) -> bool:
        return (self.fail_rate > 0 or self.responses_down
                or self.latency_ms > 0 or self.down_duration_s > 0)

    def describe(self) -> str:
        parts = []
        if self.latency_ms > 0:
            parts.append(f"latency={self.latency_ms:.0f}ms")
        if self.responses_down:
            parts.append("RESPONSES_DOWN(permanente)")
        if self.down_duration_s > 0:
            parts.append(f"down_window=[{self.down_start_s:.0f}s,"
                         f"{self.down_start_s + self.down_duration_s:.0f}s]")
        if self.fail_rate > 0:
            parts.append(f"fail_rate={self.fail_rate}")
        return ", ".join(parts) if parts else "sin fallos"


class FaultyEngine:
    """
    Envuelve un QueryEngine e inyecta los fallos en el camino del MISS.

    Expone la misma interfaz run() que QueryEngine, de modo que CacheService
    lo usa sin enterarse. Solo se invoca en cache miss → los hits no fallan.
    """

    def __init__(self, engine, injector: FaultInjector):
        self._engine = engine
        self._injector = injector

    def run(self, query_type: str, **kwargs) -> dict:
        self._injector.apply()
        return self._engine.run(query_type, **kwargs)

"""
kafka_layer 
Componentes:
  - message.py          -> schema del mensaje JSON (query_id, retry_count, ...)
  - topics.py           -> creacion idempotente de los 3 topicos
  - producer.py         -> publica consultas generadas por TrafficGenerator
  - consumer.py         -> procesa consultas con cache + engine + retry/DLQ
  - fault_injection.py  -> simulacion de fallos del Generador de Respuestas
  - lag_monitor.py      -> mide consumer group lag (backlog) periodicamente
"""

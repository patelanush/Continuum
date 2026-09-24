import logging

from opentelemetry import trace


class TraceCorrelationFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        current = trace.get_current_span().get_span_context()
        record.trace_id = f"{current.trace_id:032x}" if current.is_valid else "-"
        record.span_id = f"{current.span_id:016x}" if current.is_valid else "-"
        return True


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format=(
            "%(asctime)s %(levelname)s %(name)s "
            "trace_id=%(trace_id)s span_id=%(span_id)s %(message)s"
        ),
        force=True,
    )
    for handler in logging.getLogger().handlers:
        handler.addFilter(TraceCorrelationFilter())

"""
OpenTelemetry telemetry emitter for the synthetic render farm.
Transmits Prometheus metrics, Loki logs, and Tempo traces to Grafana Cloud via single OTLP endpoint.
"""

from datetime import datetime, timedelta, timezone
import logging
import os
import time
from typing import Optional

from opentelemetry import metrics, trace
from opentelemetry._logs import LogRecord, SeverityNumber
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

from callsheet.farm.models import FarmState, NodeStatus, ScenarioType
from callsheet.farm.simulator import RenderFarmSimulator

logger = logging.getLogger(__name__)


def get_deployment_id() -> str:
    """Returns deployment identifier: cloud-run on Cloud Run, or local-<hostname> elsewhere."""
    dep = os.environ.get("DEPLOYMENT_ID")
    if dep:
        return dep
    if os.environ.get("K_REVISION") or os.environ.get("K_SERVICE"):
        return "cloud-run"
    import socket
    hostname = socket.gethostname() or "unknown"
    return f"local-{hostname}"


class FarmTelemetryEmitter:
    """
    Emits continuous metrics, logs, and traces for the render farm to Grafana Cloud via OTLP.
    """

    def __init__(self, simulator: RenderFarmSimulator):
        self.simulator = simulator
        self.deployment_id = get_deployment_id()
        self.resource = Resource.create({
            "service.name": "render-farm",
            "service.version": "0.1.0",
            "deployment.environment": "production",
            "deployment_id": self.deployment_id,
        })
        
        self._init_metrics()
        self._init_logs()
        self._init_traces()

    def _init_metrics(self) -> None:
        duration_view = View(
            instrument_name="render_farm_frame_duration_seconds",
            aggregation=ExplicitBucketHistogramAggregation(
                boundaries=[1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0]
            ),
        )

        self.metric_exporter = OTLPMetricExporter()
        self.metric_reader = PeriodicExportingMetricReader(
            self.metric_exporter,
            export_interval_millis=5000,
        )
        self.meter_provider = MeterProvider(
            resource=self.resource,
            metric_readers=[self.metric_reader],
            views=[duration_view],
        )
        self.meter = self.meter_provider.get_meter("render-farm-meter", "0.1.0")

        # Instruments
        self.gauge_cpu = self.meter.create_gauge(
            name="render_farm_node_cpu_utilization",
            description="CPU utilization percentage per render node (0-100)",
            unit="%",
        )
        self.gauge_mem = self.meter.create_gauge(
            name="render_farm_node_memory_bytes",
            description="Memory usage in bytes per render node",
            unit="By",
        )
        self.gauge_temp = self.meter.create_gauge(
            name="render_farm_node_temperature_celsius",
            description="Hardware die temperature in Celsius per render node",
            unit="Cel",
        )
        self.gauge_gpu = self.meter.create_gauge(
            name="render_farm_node_gpu_utilization",
            description="GPU utilization percentage per render node",
            unit="%",
        )
        self.gauge_node_status = self.meter.create_gauge(
            name="render_farm_node_status",
            description="Status code: 1=HEALTHY, 2=THROTTLED, 3=OOM_CRITICAL, 4=STANDBY, 0=OFFLINE",
            unit="1",
        )
        self.hist_frame_duration = self.meter.create_histogram(
            name="render_farm_frame_duration_seconds",
            description="Distribution of completed frame render durations in seconds",
            unit="s",
        )
        self.gauge_shot_progress = self.meter.create_gauge(
            name="render_farm_shot_progress_ratio",
            description="Progress completion ratio of active shot (0.0 to 1.0)",
            unit="1",
        )
        self.gauge_shot_margin = self.meter.create_gauge(
            name="render_farm_shot_deadline_margin_hours",
            description="Buffer hours remaining before contractual delivery deadline (negative = slipping)",
            unit="h",
        )

    def _init_logs(self) -> None:
        self.log_exporter = OTLPLogExporter()
        self.logger_provider = LoggerProvider(resource=self.resource)
        self.logger_provider.add_log_record_processor(
            SimpleLogRecordProcessor(self.log_exporter)
        )
        self.otlp_logger = self.logger_provider.get_logger("render-farm-logger", "0.1.0")

    def _init_traces(self) -> None:
        self.trace_exporter = OTLPSpanExporter()
        self.tracer_provider = TracerProvider(resource=self.resource)
        self.tracer_provider.add_span_processor(
            SimpleSpanProcessor(self.trace_exporter)
        )
        self.tracer = self.tracer_provider.get_tracer("render-farm-tracer", "0.1.0")

    def emit_metrics_tick(self) -> None:
        """Collects current simulator state and emits gauges to Grafana Cloud."""
        farm = self.simulator.state
        now = datetime.now(timezone.utc)

        # 1. Emit node metrics
        for node_id, node in farm.nodes.items():
            labels = {
                "node_id": node.id,
                "node_name": node.name,
                "gpu_type": node.gpu_type,
                "deployment_id": self.deployment_id,
            }
            
            status_map = {
                NodeStatus.HEALTHY: 1,
                NodeStatus.THROTTLED: 2,
                NodeStatus.OOM_CRITICAL: 3,
                NodeStatus.STANDBY: 4,
                NodeStatus.QUARANTINED: 5,
                NodeStatus.OFFLINE: 0,
            }
            status_int = status_map.get(node.status, 0)

            self.gauge_cpu.set(node.cpu_utilization, labels)
            self.gauge_mem.set(node.memory_bytes_used, labels)
            self.gauge_temp.set(node.temperature_celsius, labels)
            self.gauge_gpu.set(node.gpu_utilization, labels)
            self.gauge_node_status.set(status_int, labels)

            # If node is throttled or overheating, emit a structured thermal log and throttled trace
            if node.status == NodeStatus.THROTTLED or node.temperature_celsius > node.thermal_limit_celsius:
                self.emit_log(
                    f"CRITICAL: Thermal junction temperature on {node.id} reached {node.temperature_celsius:.1f}C (threshold: {node.thermal_limit_celsius:.1f}C). Hardware clock down-throttled to 800MHz.",
                    level="WARN",
                    node_id=node.id,
                    shot_code="118",
                    show_id="show-aethelgard",
                )
                self.emit_frame_trace(
                    node_id=node.id,
                    shot_code="118",
                    show_name="Chronicles of Aethelgard: Episode 6",
                    frame_number=node.current_frame or 1105,
                    duration_seconds=120.0,
                    is_throttled=True,
                    temperature_celsius=node.temperature_celsius,
                )

        # 2. Emit active shot metrics (bounded set to prevent cardinality churn)
        for shot_id, shot in farm.shots.items():
            if shot.status in [NodeStatus.STANDBY]:
                continue
            
            show = farm.shows.get(shot.show_id)
            deadline = show.delivery_deadline if show else now
            est_seconds_remaining = shot.estimated_time_remaining_seconds()
            est_completion = now + timedelta(seconds=est_seconds_remaining)
            margin_hours = (deadline - est_completion).total_seconds() / 3600.0

            shot_labels = {
                "shot_id": shot.id,
                "shot_code": shot.shot_code,
                "show_id": shot.show_id,
                "sequence": shot.sequence,
                "deployment_id": self.deployment_id,
            }

            self.gauge_shot_progress.set(shot.progress_ratio, shot_labels)
            self.gauge_shot_margin.set(round(margin_hours, 2), shot_labels)

        # Force flush metrics to exporter
        self.metric_reader.force_flush()

    def emit_log(
        self,
        message: str,
        level: str = "INFO",
        node_id: Optional[str] = None,
        shot_code: Optional[str] = None,
        show_id: Optional[str] = None,
        frame_number: Optional[int] = None,
    ) -> None:
        """Emits a structured log record to Loki via OTLP."""
        severity_map = {
            "DEBUG": SeverityNumber.DEBUG,
            "INFO": SeverityNumber.INFO,
            "WARN": SeverityNumber.WARN,
            "WARNING": SeverityNumber.WARN,
            "ERROR": SeverityNumber.ERROR,
            "CRITICAL": SeverityNumber.FATAL,
        }
        severity_num = severity_map.get(level.upper(), SeverityNumber.INFO)

        attributes = {
            "service.name": "render-farm",
            "log.level": level.upper(),
            "deployment_id": self.deployment_id,
        }
        if node_id:
            attributes["node_id"] = node_id
        if shot_code:
            attributes["shot_code"] = shot_code
        if show_id:
            attributes["show_id"] = show_id
        if frame_number is not None:
            attributes["frame_number"] = frame_number

        log_record = LogRecord(
            timestamp=int(time.time() * 1e9),
            observed_timestamp=int(time.time() * 1e9),
            trace_id=trace.get_current_span().get_span_context().trace_id or 0,
            span_id=trace.get_current_span().get_span_context().span_id or 0,
            body=message,
            severity_number=severity_num,
            severity_text=level.upper(),
            attributes=attributes,
        )
        self.otlp_logger.emit(log_record)

    def emit_frame_trace(
        self,
        node_id: str,
        shot_code: str,
        show_name: str,
        frame_number: int,
        duration_seconds: float,
        is_throttled: bool = False,
        temperature_celsius: float | None = None,
    ) -> None:
        """
        Emits a detailed execution trace for a completed frame to Tempo via OTLP.
        Explicitly portions durations:
        - Load stage: 2.0s constant
        - Denoise stage: 1.0s constant
        - Raytrace volumetrics stage: absorbs the remaining duration (17.0s normal vs 117.0s throttled).
        """
        now = time.time()
        t_start_ns = int((now - duration_seconds) * 1e9)
        t_end_ns = int(now * 1e9)

        root_attrs = {
            "shot.code": shot_code,
            "shot_code": shot_code,
            "frame.number": frame_number,
            "node.id": node_id,
            "node_id": node_id,
            "show.name": show_name,
            "duration.seconds": duration_seconds,
            "node.throttled": is_throttled,
            "deployment_id": self.deployment_id,
        }
        if temperature_celsius is not None:
            root_attrs["node.temperature_celsius"] = round(temperature_celsius, 1)

        # 1. Root span
        root_span = self.tracer.start_span(
            f"render_frame_sh{shot_code}_{frame_number}",
            start_time=t_start_ns,
            attributes=root_attrs,
        )
        ctx = trace.set_span_in_context(root_span)

        # 2. Stage 1: Load geometry and textures (constant 2.0s)
        load_dur = min(2.0, max(0.5, duration_seconds * 0.1))
        t1_start_ns = t_start_ns
        t1_end_ns = t_start_ns + int(load_dur * 1e9)
        
        s1 = self.tracer.start_span(
            "load_geometry_and_textures",
            context=ctx,
            start_time=t1_start_ns,
            attributes={
                "assets.count": 42,
                "textures.size_mb": 1450,
                "node_id": node_id,
                "node.id": node_id,
                "deployment_id": self.deployment_id,
            },
        )
        s1.end(end_time=t1_end_ns)

        # 3. Stage 3: Denoise and color grade (constant 1.0s)
        denoise_dur = min(1.0, max(0.2, duration_seconds * 0.05))
        t3_start_ns = t_end_ns - int(denoise_dur * 1e9)
        t3_end_ns = t_end_ns

        # 4. Stage 2: Raytrace volumetrics (takes the bulk of render time)
        t2_start_ns = t1_end_ns
        t2_end_ns = t3_start_ns
        raytrace_dur = max(0.1, (t2_end_ns - t2_start_ns) / 1e9)

        s2_attrs = {
            "samples_per_pixel": 1024,
            "bounces": 8,
            "thermal_throttled": is_throttled,
            "node_id": node_id,
            "node.id": node_id,
            "duration_seconds": round(raytrace_dur, 2),
            "deployment_id": self.deployment_id,
        }
        if is_throttled:
            s2_attrs["warning"] = "CPU clock frequency throttled to 800MHz due to high thermal junction temp"
            s2_attrs["bottleneck"] = "hardware_thermal_throttle"
            if temperature_celsius is not None:
                s2_attrs["temperature_celsius"] = round(temperature_celsius, 1)

        s2 = self.tracer.start_span(
            "raytrace_volumetrics_pass",
            context=ctx,
            start_time=t2_start_ns,
            attributes=s2_attrs,
        )
        s2.end(end_time=t2_end_ns)

        # End Stage 3
        s3 = self.tracer.start_span(
            "denoise_and_color_grade",
            context=ctx,
            start_time=t3_start_ns,
            attributes={
                "denoiser": "OptiX",
                "lut": "ACEScg",
                "node_id": node_id,
                "node.id": node_id,
                "deployment_id": self.deployment_id,
            },
        )
        s3.end(end_time=t3_end_ns)

        # End Root Span
        root_span.end(end_time=t_end_ns)

        self.hist_frame_duration.record(
            duration_seconds,
            {"node_id": node_id, "shot_code": shot_code, "deployment_id": self.deployment_id}
        )

    def process_events(self, events: list[dict]) -> None:
        """Processes events generated by the simulation tick and transmits logs/traces."""
        for ev in events:
            ev_type = ev.get("type")
            node_id = ev.get("node_id")
            shot_code = ev.get("shot_code")
            show_id = ev.get("show_id")
            frame_num = ev.get("frame_number")
            duration = ev.get("duration_seconds", 20.0)

            node = self.simulator.state.nodes.get(node_id)
            is_throttled = node.status == NodeStatus.THROTTLED if node else False
            show = self.simulator.state.shows.get(show_id)
            show_name = show.name if show else show_id

            if ev_type == "FRAME_COMPLETED":
                self.emit_frame_trace(
                    node_id=node_id,
                    shot_code=shot_code,
                    show_name=show_name,
                    frame_number=frame_num,
                    duration_seconds=duration,
                    is_throttled=is_throttled,
                )

                if is_throttled:
                    self.emit_log(
                        f"Frame {frame_num} rendered on {node_id} with DEGRADED PERFORMANCE ({duration:.1f}s). Thermal throttle active.",
                        level="WARN",
                        node_id=node_id,
                        shot_code=shot_code,
                        show_id=show_id,
                        frame_number=frame_num,
                    )
                else:
                    self.emit_log(
                        f"Frame {frame_num} rendered on {node_id} successfully in {duration:.1f}s.",
                        level="INFO",
                        node_id=node_id,
                        shot_code=shot_code,
                        show_id=show_id,
                        frame_number=frame_num,
                    )

            elif ev_type == "SHOT_COMPLETED":
                self.emit_log(
                    f"Shot {shot_code} completed all frames on {node_id}. Ready for compositing pass.",
                    level="INFO",
                    node_id=node_id,
                    shot_code=shot_code,
                    show_id=show_id,
                )

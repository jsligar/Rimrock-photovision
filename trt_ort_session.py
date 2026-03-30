"""
trt_ort_session.py — TensorRT-backed drop-in for onnxruntime.InferenceSession.

Usage (must happen before any insightface import):
    from trt_ort_session import patch_onnxruntime
    patch_onnxruntime()

On first use for each ONNX model, builds a TRT FP16 engine via trtexec and
caches it to TRT_ENGINE_CACHE.  Subsequent loads deserialize the cached engine.

Falls back to the original ORT session if TRT build or load fails.
"""
from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path
from typing import Any

import numpy as np
import torch
import tensorrt as trt

log = logging.getLogger(__name__)

TRT_ENGINE_CACHE = Path(os.getenv("TRT_ENGINE_CACHE", "/root/.insightface/trt_engines"))

# Capture the real ORT InferenceSession at import time (before any patching).
import onnxruntime as _ort
_OriginalOrtSession = _ort.InferenceSession
TRTEXEC = os.getenv("TRTEXEC", "/opt/tensorrt/bin/trtexec")
BUILD_TIMEOUT = int(os.getenv("TRT_BUILD_TIMEOUT", 600))

# Dynamic shape optimization profiles for InsightFace buffalo_l models.
# Key = ONNX model stem.  Inner key = actual tensor name from the ONNX graph.
# det_10g/w600k_r50 use "input.1"; genderage/2d106det/1k3d68 use "data".
_SHAPE_PROFILES: dict[str, dict[str, dict]] = {
    "det_10g": {
        "input.1": {
            "min": (1, 3, 160, 160),
            "opt": (1, 3, 640, 640),
            "max": (1, 3, 1280, 1280),
        }
    },
    "w600k_r50": {
        "input.1": {
            "min": (1, 3, 112, 112),
            "opt": (8, 3, 112, 112),
            "max": (32, 3, 112, 112),
        }
    },
    "2d106det": {
        "data": {
            "min": (1, 3, 192, 192),
            "opt": (1, 3, 192, 192),
            "max": (8, 3, 192, 192),
        }
    },
    "1k3d68": {
        "data": {
            "min": (1, 3, 192, 192),
            "opt": (1, 3, 192, 192),
            "max": (8, 3, 192, 192),
        }
    },
    "genderage": {
        "data": {
            "min": (1, 3, 96, 96),
            "opt": (8, 3, 96, 96),
            "max": (32, 3, 96, 96),
        }
    },
}

_TRT_DTYPE_TO_TORCH = {
    trt.float32: torch.float32,
    trt.float16: torch.float16,
    trt.int32: torch.int32,
    trt.int8: torch.int8,
    trt.bool: torch.bool,
}


class _NodeInfo:
    """Mimics onnxruntime.NodeArg — .name and .shape only."""

    def __init__(self, name: str, shape: list) -> None:
        self.name = name
        self.shape = shape  # Plain attribute, not a method — matches ORT NodeArg API


class TRTSession:
    """
    Drop-in replacement for onnxruntime.InferenceSession using TensorRT FP16.

    Transparent to InsightFace — same .run() / .get_inputs() / .get_outputs() API.
    """

    _logger = trt.Logger(trt.Logger.WARNING)

    def __init__(self, model_path: str, options: Any = None, providers: Any = None, **kwargs: Any) -> None:
        model_path = str(model_path)
        stem = Path(model_path).stem
        TRT_ENGINE_CACHE.mkdir(parents=True, exist_ok=True)
        engine_path = TRT_ENGINE_CACHE / f"{stem}_fp16.engine"

        try:
            if not engine_path.exists():
                log.info("[TRT] Building engine for %s (one-time, ~2-5 min)...", stem)
                _build_engine(model_path, engine_path, stem)
            else:
                log.info("[TRT] Loading cached engine: %s", engine_path.name)

            runtime = trt.Runtime(self._logger)
            with open(engine_path, "rb") as f:
                self._engine = runtime.deserialize_cuda_engine(f.read())

            self._context = self._engine.create_execution_context()
            self._stream = torch.cuda.Stream()

            # Collect tensor names
            self._input_names: list[str] = []
            self._output_names: list[str] = []
            for i in range(self._engine.num_io_tensors):
                name = self._engine.get_tensor_name(i)
                mode = self._engine.get_tensor_mode(name)
                if mode == trt.TensorIOMode.INPUT:
                    self._input_names.append(name)
                else:
                    self._output_names.append(name)

            self._stem = stem
            self._failed = False
            # Attributes accessed directly by InsightFace internals
            self._providers = ["TensorrtExecutionProvider"]
            self._provider_options = [{}]
            log.info("[TRT] Engine ready: %s  inputs=%s  outputs=%s",
                     stem, self._input_names, self._output_names)

        except Exception as exc:
            log.warning("[TRT] Failed to load TRT engine for %s, falling back to ORT CPU: %s",
                        stem, exc)
            self._failed = True
            self._providers = ["CPUExecutionProvider"]
            self._provider_options = [{}]
            self._fallback = _OriginalOrtSession(model_path, options)

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self, output_names: list[str] | None, input_dict: dict) -> list[np.ndarray]:
        if self._failed:
            return self._fallback.run(output_names, input_dict)

        with torch.cuda.stream(self._stream):
            input_tensors: list[torch.Tensor] = []

            for name in self._input_names:
                value = input_dict[name]
                tensor = torch.as_tensor(value, device="cuda")
                input_tensors.append(tensor)
                self._context.set_input_shape(name, tuple(tensor.shape))
                self._context.set_tensor_address(name, tensor.data_ptr())

            wanted = output_names if output_names else self._output_names
            output_tensors: dict[str, torch.Tensor] = {}
            for name in wanted:
                shape = tuple(self._context.get_tensor_shape(name))
                dtype = _TRT_DTYPE_TO_TORCH.get(
                    self._engine.get_tensor_dtype(name), torch.float32
                )
                out = torch.empty(shape, dtype=dtype, device="cuda")
                output_tensors[name] = out
                self._context.set_tensor_address(name, out.data_ptr())

            self._context.execute_async_v3(self._stream.cuda_stream)
            self._stream.synchronize()

        return [output_tensors[n].cpu().numpy() for n in wanted]

    def get_inputs(self) -> list[_NodeInfo]:
        if self._failed:
            return self._fallback.get_inputs()
        result = []
        for name in self._input_names:
            raw = list(self._engine.get_tensor_shape(name))
            # Replace -1 with a string so InsightFace treats dims as dynamic
            shape = ["dynamic" if d < 0 else d for d in raw]
            result.append(_NodeInfo(name, shape))
        return result

    def get_outputs(self) -> list[_NodeInfo]:
        if self._failed:
            return self._fallback.get_outputs()
        result = []
        for name in self._output_names:
            raw = list(self._engine.get_tensor_shape(name))
            shape = ["dynamic" if d < 0 else d for d in raw]
            result.append(_NodeInfo(name, shape))
        return result

    def set_providers(self, providers: list[str], provider_options: Any = None) -> None:
        # Silently ignore — TRT is already the provider
        pass


# ── Engine builder ─────────────────────────────────────────────────────────────

def _build_engine(onnx_path: str, engine_path: Path, stem: str) -> None:
    cmd = [
        TRTEXEC,
        f"--onnx={onnx_path}",
        f"--saveEngine={engine_path}",
        "--fp16",
        "--noTF32",
    ]

    profile = _SHAPE_PROFILES.get(stem)
    if profile:
        for input_name, shapes in profile.items():
            min_s = "x".join(str(d) for d in shapes["min"])
            opt_s = "x".join(str(d) for d in shapes["opt"])
            max_s = "x".join(str(d) for d in shapes["max"])
            cmd += [
                f"--minShapes={input_name}:{min_s}",
                f"--optShapes={input_name}:{opt_s}",
                f"--maxShapes={input_name}:{max_s}",
            ]

    log.info("[TRT] trtexec: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=BUILD_TIMEOUT)
    if result.returncode != 0:
        raise RuntimeError(
            f"trtexec failed for {stem}:\n{result.stderr[-3000:]}"
        )
    log.info("[TRT] Engine built: %s", engine_path)


# ── Patch onnxruntime ─────────────────────────────────────────────────────────

def patch_onnxruntime() -> None:
    """
    Replace onnxruntime.InferenceSession with TRTSession.
    Call this once before any insightface import.
    The original session is preserved as onnxruntime._OriginalInferenceSession
    for use in the CPU fallback path.
    """
    import onnxruntime as ort

    if getattr(ort, "_trt_patched", False):
        return  # Already patched

    ort._OriginalInferenceSession = ort.InferenceSession
    ort.InferenceSession = TRTSession
    ort._trt_patched = True
    log.info("[TRT] onnxruntime.InferenceSession → TRTSession (TensorRT FP16)")

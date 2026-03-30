#!/usr/bin/env python3
"""TensorRT readiness check for Rimrock (Jetson Orin Nano Super / JetPack 6).

Run on the Jetson BEFORE installing the photo pipeline:
    python3 scripts/test_tensorrt.py

Checks:
  1. CUDA availability (torch + nvidia-smi)
  2. TensorRT library import + version
  3. onnxruntime GPU providers (CUDA & TensorRT)
  4. Build + run a tiny TensorRT engine (end-to-end sanity)
  5. (Optional) YOLOv8s TensorRT export dry-run
"""

import subprocess
import sys
import shutil
from pathlib import Path

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
WARN = "\033[93m!\033[0m"
BOLD = "\033[1m"
RESET = "\033[0m"

results = []


def check(name, passed, detail=""):
    tag = PASS if passed else FAIL
    results.append((name, passed))
    msg = f"  {tag} {name}"
    if detail:
        msg += f"  —  {detail}"
    print(msg)
    return passed


def section(title):
    print(f"\n{BOLD}[{title}]{RESET}")


# ── 1. CUDA ──────────────────────────────────────────────────────────────────
section("CUDA")

# nvidia-smi
nvsmi = shutil.which("nvidia-smi") or shutil.which("tegrastats")
if nvsmi and "nvidia-smi" in nvsmi:
    try:
        out = subprocess.check_output(["nvidia-smi"], text=True, timeout=10)
        driver_line = [l for l in out.splitlines() if "Driver Version" in l]
        check("nvidia-smi", True, driver_line[0].strip() if driver_line else "OK")
    except Exception as e:
        check("nvidia-smi", False, str(e))
else:
    # Jetson doesn't always have nvidia-smi; tegrastats is the alternative
    check("nvidia-smi", True, "Jetson uses tegrastats (nvidia-smi may not exist)")

# PyTorch CUDA
try:
    import torch
    cuda_ok = torch.cuda.is_available()
    detail = f"torch {torch.__version__}, CUDA {torch.version.cuda}" if cuda_ok else "CUDA not available"
    if cuda_ok:
        detail += f", device: {torch.cuda.get_device_name(0)}"
    check("torch.cuda", cuda_ok, detail)
except ImportError:
    check("torch.cuda", False, "torch not installed (install JetPack PyTorch wheel first)")

# ── 2. TensorRT library ─────────────────────────────────────────────────────
section("TensorRT")

try:
    import tensorrt as trt
    check("import tensorrt", True, f"version {trt.__version__}")
except ImportError:
    check("import tensorrt", False,
          "apt install python3-libnvinfer or pip install tensorrt (JetPack ships it)")

# trtexec binary
trtexec = shutil.which("trtexec") or Path("/usr/src/tensorrt/bin/trtexec")
if isinstance(trtexec, Path):
    trtexec = trtexec if trtexec.exists() else None
check("trtexec binary", trtexec is not None,
      str(trtexec) if trtexec else "not found in PATH or /usr/src/tensorrt/bin/")

# ── 3. ONNX Runtime GPU providers ───────────────────────────────────────────
section("ONNX Runtime")

try:
    import onnxruntime as ort
    providers = ort.get_available_providers()
    has_cuda = "CUDAExecutionProvider" in providers
    has_trt = "TensorrtExecutionProvider" in providers
    check("onnxruntime-gpu", True, f"version {ort.__version__}")
    check("CUDAExecutionProvider", has_cuda, ", ".join(providers))
    check("TensorrtExecutionProvider", has_trt,
          "InsightFace can use TRT acceleration" if has_trt else
          "pip install onnxruntime-gpu with TRT support")
except ImportError:
    check("onnxruntime-gpu", False, "not installed")

# ── 4. Build + run a tiny TensorRT engine ────────────────────────────────────
section("TensorRT Engine (end-to-end)")

try:
    import tensorrt as trt
    import numpy as np

    TRT_LOGGER = trt.Logger(trt.Logger.WARNING)
    builder = trt.Builder(TRT_LOGGER)
    network = builder.create_network(1 << int(trt.NetworkDefinitionCreationFlag.EXPLICIT_BATCH))
    config = builder.create_builder_config()
    config.set_memory_pool_limit(trt.MemoryPoolType.WORKSPACE, 1 << 20)  # 1 MB

    # Minimal network: input -> identity -> output
    inp = network.add_input("x", trt.float32, (1, 4))
    identity = network.add_identity(inp)
    identity.get_output(0).name = "y"
    network.mark_output(identity.get_output(0))

    engine_bytes = builder.build_serialized_network(network, config)
    if engine_bytes is None:
        check("build engine", False, "builder returned None")
    else:
        # TRT 10.x returns IHostMemory; get size via .nbytes or fallback
        nbytes = getattr(engine_bytes, 'nbytes', None)
        if nbytes is None:
            nbytes = len(bytes(engine_bytes))

        runtime = trt.Runtime(TRT_LOGGER)
        engine = runtime.deserialize_cuda_engine(engine_bytes)
        context = engine.create_execution_context()

        # Allocate and run
        import torch as _torch
        inp_tensor = _torch.tensor([[1.0, 2.0, 3.0, 4.0]], device="cuda")
        out_tensor = _torch.empty((1, 4), device="cuda")
        context.set_tensor_address("x", inp_tensor.data_ptr())
        context.set_tensor_address("y", out_tensor.data_ptr())
        context.execute_async_v3(_torch.cuda.current_stream().cuda_stream)
        _torch.cuda.synchronize()

        match = _torch.allclose(inp_tensor, out_tensor)
        check("build engine", True, f"serialized {nbytes} bytes")
        check("inference", match, f"output={out_tensor.cpu().numpy().tolist()}")
except Exception as e:
    check("TRT engine test", False, str(e))

# ── 5. YOLOv8 TensorRT export check (informational) ─────────────────────────
section("YOLOv8 TensorRT Export (info only)")

try:
    from ultralytics import YOLO
    check("ultralytics import", True)
    print(f"  {WARN} To export YOLOv8s to TensorRT engine (takes ~2-5 min):")
    print(f"      python3 -c \"from ultralytics import YOLO; YOLO('yolov8s.pt').export(format='engine', imgsz=640, half=True)\"")
    print(f"      This creates yolov8s.engine — set YOLO_MODEL in config.py to use it.")
except ImportError:
    check("ultralytics import", False, "not installed yet (expected pre-install)")

# ── Summary ──────────────────────────────────────────────────────────────────
section("Summary")
passed = sum(1 for _, p in results if p)
total = len(results)
all_ok = passed == total
print(f"  {passed}/{total} checks passed")

critical = ["torch.cuda", "import tensorrt", "build engine", "inference"]
critical_fails = [n for n, p in results if n in critical and not p]
if critical_fails:
    print(f"\n  {FAIL} Critical failures: {', '.join(critical_fails)}")
    print(f"    These must pass before installing the photo pipeline.")
    sys.exit(1)
elif all_ok:
    print(f"\n  {PASS} TensorRT is ready. Safe to install the photo pipeline.")
else:
    print(f"\n  {WARN} Non-critical items missing — pipeline will still work but may be slower.")
    sys.exit(0)

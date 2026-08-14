# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 X.AI Corp.
"""CUDA kernels used by ``xrex``.

``xrex`` needs three generic array primitives that xAI maintains as CUDA
kernels: an Adler-32 checksum (checkpoint integrity), a 1-D int32 dedupe
(the training embedding compressor) and a batched top-k (the retrieval
serving path). This directory is the single implementation of all three.

One more kernel lives here with a different contract: ``async_emb``
(pipelined embedding communication) has NO reference path — it raises a loud,
named ImportError when its compiled extension is absent, and its callers
treat that as "the feature is off" (``use_async_emb``).

Each of the three primitive kernels is a subpackage with the same two-layer
shape:

* a **reference implementation in pure JAX (or zlib) that always works** — no
  compiler, no CUDA, no extension module. This is the path that runs unless a
  compiled extension is present, and it is what an installation without
  ``nvcc`` gets;
* the **CUDA/C++ source** under ``<kernel>/src/``, with the shared XLA-FFI
  helpers under ``xla_utils/``. Building it produces a nanobind extension named
  after the file (``adler32_api``, ``unique_api``, ``top_k_by_key_api``, ...),
  and each subpackage tries to import that extension from its own ``src/`` at
  import time. When the import succeeds the kernel is registered as an XLA FFI
  target and used instead of the reference; when it fails the reference stands.

The FFI target names are prefixed ``xrex_``.

**The extensions are built out of tree, never by ``uv sync``.** The compiled
path is optional by design, so the package installs and the reference kernels
run on a box with no ``nvcc``. To compile one yourself you need ``nvcc``, the
XLA FFI headers (``xla/ffi/api/ffi.h``, from jaxlib), nanobind, and — for
``unique`` and ``top_k_by_key`` — CUB/Thrust/CUTLASS from the CUDA toolkit.
Compile ``<kernel>/src/*.cu`` and ``*.cc`` into a shared object named for its
``NB_MODULE`` and drop it in ``<kernel>/src/``, with the repository root on the
include path so ``xrex/cuda/xla_utils/...`` resolves (``async_emb`` localizes
its includes and needs only its own ``src/``).
"""

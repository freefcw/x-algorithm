# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 X.AI Corp.
import zlib

import jax
import numpy as np
from jax import numpy as jnp

_BASE = 65521

_FFI_BACKENDS: set[str] = set()

try:
    from xrex.cuda.adler32.src import adler32_api
except ImportError:
    pass
else:
    jax.ffi.register_ffi_target("xrex_adler32", adler32_api.adler32(), platform="CUDA")
    _FFI_BACKENDS.add("gpu")

try:
    from xrex.cuda.adler32.src import adler32_cpu_api
except ImportError:
    pass
else:
    jax.ffi.register_ffi_target("xrex_adler32", adler32_cpu_api.adler32(), platform="cpu")
    _FFI_BACKENDS.add("cpu")


def _adler32_reference(data: jax.Array) -> jax.Array:
    def _host(arr):
        return np.uint32(zlib.adler32(np.asarray(arr).tobytes()))

    return jax.pure_callback(_host, jax.ShapeDtypeStruct((), jnp.uint32), data)


def _adler32_ffi(data: jax.Array) -> jax.Array:
    call = jax.ffi.ffi_call(
        "xrex_adler32",
        (jax.ShapeDtypeStruct((), np.uint32), jax.ShapeDtypeStruct((), np.uint32)),
        vmap_method="broadcast_all",
    )
    a, b = call(data)
    a %= _BASE
    b %= _BASE
    return (b << 16) | a


@jax.jit
def adler32(data: jax.Array) -> jax.Array:
    if jax.default_backend() in _FFI_BACKENDS:
        return _adler32_ffi(data)
    return _adler32_reference(data)

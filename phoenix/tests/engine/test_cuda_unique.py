import jax.numpy as jnp
import numpy as np

import xrex.cuda.unique as unique_module


def test_reference_unique_returns_inverse_indices(monkeypatch):
    monkeypatch.setattr(unique_module, "unique_api", None)
    values = jnp.array([4, 2, 4, 3], dtype=jnp.int32)

    unique_values, inverse_indices = unique_module.unique(
        values,
        return_inverse=True,
        size=6,
        fill_value=-1,
    )

    np.testing.assert_array_equal(np.asarray(unique_values), [2, 3, 4, -1, -1, -1])
    np.testing.assert_array_equal(np.asarray(inverse_indices), [2, 0, 2, 1])
    assert inverse_indices.dtype == values.dtype


def test_reference_unique_returns_placeholder_without_inverse_indices(monkeypatch):
    monkeypatch.setattr(unique_module, "unique_api", None)
    values = jnp.array([4, 2, 4, 3], dtype=jnp.int32)

    unique_values, inverse_indices = unique_module.unique(
        values,
        return_inverse=False,
        size=6,
        fill_value=-1,
    )

    np.testing.assert_array_equal(np.asarray(unique_values), [2, 3, 4, -1, -1, -1])
    np.testing.assert_array_equal(np.asarray(inverse_indices), np.zeros_like(np.asarray(values)))
    assert inverse_indices.shape == values.shape
    assert inverse_indices.dtype == values.dtype

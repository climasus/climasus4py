"""R's random number generator, enough to reproduce ``set.seed()`` + ``rnorm()``.

Why this module exists: ``climasus4r`` draws Monte Carlo samples in
``.compute_mc_uncertainty()`` under a fixed ``set.seed(2024L)``, so its
confidence intervals are deterministic — and reproducing them requires
reproducing R's stream exactly. NumPy cannot: it seeds MT19937 differently
and transforms uniforms into normals by a different method. Seeding
``numpy.random.RandomState(2024)`` yields a completely unrelated sequence.

Two pieces, both taken from R's C sources:

- ``qnorm`` — Wichura's AS 241, from ``src/nmath/qnorm.c``. Needed because
  R's normals come from *inverting* the CDF, so any difference in the
  inverse shows up in every draw. SciPy's ``ndtri`` (Cephes) is accurate to
  about 1e-16 but disagrees with R in the last bits; measured against R over
  35 probabilities spanning 1e-10 to 1-1e-10, AS 241 matched all 35 exactly
  while ``ndtri`` was off by up to 1.8e-15.
- ``RRandom`` — Mersenne-Twister plus R's ``set.seed`` scrambling and R's
  inversion-based ``norm_rand``, from ``src/main/RNG.c``.

Verified against R 4.6.0 after ``set.seed(2024L)``: the first eight
``runif`` and the first eight ``rnorm`` values are **bit-identical**.

Only the pieces the port needs are here. This is not a general R RNG: the
other ``RNGkind`` options, the other ``normal.kind`` transforms, and
``rnorm``'s two-argument forms beyond mean/sd scaling are absent.
"""

from __future__ import annotations

import math

import numpy as np

__all__ = ["qnorm", "RRandom"]

# --- Mersenne-Twister constants, as R has them -----------------------------
_N, _M = 624, 397
_MATRIX_A = 0x9908B0DF
_UPPER, _LOWER = 0x80000000, 0x7FFFFFFF
_TEMP_B, _TEMP_C = 0x9D2C5680, 0xEFC60000
_MASK32 = 0xFFFFFFFF
# R's i2_32m1, used by fixup() to keep 0 and 1 out of the open interval
_I2_32M1 = 2.328306437080797e-10
_TWO_POW_M32 = 2.3283064365386963e-10
# norm_rand's INVERSION branch combines two uniforms into 2^27 levels
_BIG = 134217728


def qnorm(p: float) -> float:
    """Standard-normal quantile, matching R's ``qnorm(p)`` bit for bit.

    Wichura's AS 241 as R implements it: a rational approximation on
    ``|p - 0.5| <= 0.425`` and two more on the tails, split at
    ``sqrt(-log(min(p, 1-p))) = 5``.

    Args:
        p: Probability. ``<= 0`` gives ``-inf`` and ``>= 1`` gives ``inf``,
            as R does.

    Returns:
        The quantile.
    """
    if math.isnan(p):
        return math.nan
    if p <= 0.0:
        return -math.inf
    if p >= 1.0:
        return math.inf

    q = p - 0.5
    if abs(q) <= 0.425:
        r = 0.180625 - q * q
        num = (((((((r * 2509.0809287301226727 +
                     33430.575583588128105) * r + 67265.770927008700853) * r +
                   45921.953931549871457) * r + 13731.693765509461125) * r +
                 1971.5909503065514427) * r + 133.14166789178437745) * r +
               3.387132872796366608)
        den = (((((((r * 5226.495278852854561 +
                     28729.085735721942674) * r + 39307.89580009271061) * r +
                   21213.794301586595867) * r + 5394.1960214247511077) * r +
                 687.1870074920579083) * r + 42.313330701600911252) * r + 1.0)
        return q * num / den

    r = (1.0 - p) if q > 0 else p
    r = math.sqrt(-math.log(r))

    if r <= 5.0:
        r -= 1.6
        num = (((((((r * 7.7454501427834140764e-4 +
                     0.0227238449892691845833) * r + 0.24178072517745061177) *
                   r + 1.27045825245236838258) * r +
                  3.64784832476320460504) * r + 5.7694972214606914055) *
                r + 4.6303378461565452959) * r + 1.42343711074968357734)
        den = (((((((r * 1.05075007164441684324e-9 +
                     5.475938084995344946e-4) * r +
                    0.0151986665636164571966) * r +
                   0.14810397642748007459) * r + 0.68976733498510000455) *
                 r + 1.6763848301838038494) * r +
                2.05319162663775882187) * r + 1.0)
    elif r <= 27.0:
        r -= 5.0
        num = (((((((r * 2.01033439929228813265e-7 +
                     2.71155556874348757815e-5) * r +
                    0.0012426609473880784386) * r +
                   0.026532189526576123093) * r + 0.29656057182850489123) *
                 r + 1.7848265399172913358) * r + 5.4637849111641143699) *
               r + 6.6579046435011037772)
        den = (((((((r * 2.04426310338993978564e-15 +
                     1.4215117583164458887e-7) * r +
                    1.8463183175100546818e-5) * r +
                   7.868691311456132591e-4) * r + 0.0148753612908506148525) *
                 r + 0.13692988092273580531) * r +
                0.59983220655588793769) * r + 1.0)
    else:
        # Beyond r = 27, p is closer to a boundary than exp(-729). The
        # inversion in norm_rand quantises to 2^27 levels, so it can never
        # reach here; R has a further asymptotic branch we do not need.
        return -math.inf if q < 0 else math.inf

    val = num / den
    return -val if q < 0 else val


_qnorm_vec = np.vectorize(qnorm, otypes=[np.float64])


class RRandom:
    """R's uniform and normal streams after ``set.seed(seed)``.

    The state is per instance, so two instances with the same seed give the
    same sequence — which is what mirrors R calling ``set.seed()`` again.

    Example::

        rng = RRandom(2024)
        rng.rnorm(8)   # identical to R's set.seed(2024); rnorm(8)
    """

    def __init__(self, seed: int):
        s = seed & _MASK32
        # RNG_Init does 50 scrambling steps BEFORE filling the state. Without
        # them nothing matches: they are why RandomState(2024) is unrelated.
        for _ in range(50):
            s = (69069 * s + 1) & _MASK32
        estado = np.empty(_N + 1, dtype=np.uint64)
        for j in range(_N + 1):
            s = (69069 * s + 1) & _MASK32
            estado[j] = s
        # i_seed[0] holds mti; FixupSeeds(initial=TRUE) forces it to 624,
        # which makes the first draw generate a fresh block.
        self._mt = estado[1:].astype(np.uint64)
        self._mti = _N

    def _twist(self) -> None:
        """Regenerate the 624-word block, vectorised but bit-faithful.

        The recurrence is ``mt[i] = mt[(i + 397) mod 624] ^ f(mt[i], mt[i+1])``.
        R's C code runs it in place, which makes it **self-referential**: for
        ``i >= 454`` the feed word ``mt[i - 227]`` was already rewritten
        earlier in the same loop. A single vectorised pass over the old state
        therefore gives wrong words from 455 on — measured against R, the
        first block matched except its second-to-last word and everything
        past draw 1246 diverged.

        The fix without giving up vectorisation: the wrapped feed has a fixed
        lag of 227, so three chunks of 227 suffice, each reading only words
        already finalised by an earlier chunk.

        ``mt[i]`` and ``mt[i+1]`` always come from the old state — the loop
        writes at ``i`` and never reads below it — except the final word,
        whose ``mt[i+1]`` wraps to the freshly written ``mt[0]``.
        """
        mt = self._mt
        antigo = mt.copy()
        um, A, zero = np.uint64(1), np.uint64(_MATRIX_A), np.uint64(0)
        lag = _N - _M  # 227

        def passo(alto, baixo, feed):
            y = (alto & _UPPER) | (baixo & _LOWER)
            return feed ^ (y >> um) ^ np.where(y & um, A, zero)

        # i in [0, 227): feed is mt[i+397], still untouched
        mt[:lag] = passo(antigo[:lag], antigo[1:lag + 1], antigo[_M:])
        # i in [227, 454): feed is mt[i-227], written by the chunk above
        mt[lag:2 * lag] = passo(antigo[lag:2 * lag], antigo[lag + 1:2 * lag + 1],
                                mt[:lag])
        # i in [454, 623): feed is mt[i-227], written by the chunk above
        mt[2 * lag:_N - 1] = passo(antigo[2 * lag:_N - 1],
                                   antigo[2 * lag + 1:_N],
                                   mt[lag:_N - 1 - lag])
        # last word: mt[i+1] wraps to the new mt[0]
        mt[_N - 1] = passo(antigo[_N - 1], mt[0], mt[_M - 1])
        self._mti = 0

    def _raw(self, n: int) -> np.ndarray:
        """`n` tempered 32-bit words, straight from the generator."""
        saida = np.empty(n, dtype=np.uint64)
        feito = 0
        while feito < n:
            if self._mti >= _N:
                self._twist()
            tomar = min(n - feito, _N - self._mti)
            saida[feito:feito + tomar] = \
                self._mt[self._mti:self._mti + tomar]
            self._mti += tomar
            feito += tomar
        y = saida
        y = y ^ (y >> np.uint64(11))
        y = (y ^ ((y << np.uint64(7)) & np.uint64(_TEMP_B))) & np.uint64(_MASK32)
        y = (y ^ ((y << np.uint64(15)) & np.uint64(_TEMP_C))) & np.uint64(_MASK32)
        y = y ^ (y >> np.uint64(18))
        return y

    def unif_rand(self, n: int = 1) -> np.ndarray:
        """`n` uniforms, matching R's ``runif(n)``."""
        x = self._raw(n).astype(np.float64) * _TWO_POW_M32
        # fixup(): 0 and 1 must never come out
        np.copyto(x, 0.5 * _I2_32M1, where=x <= 0.0)
        np.copyto(x, 1.0 - 0.5 * _I2_32M1, where=x >= 1.0)
        return x

    def norm_rand(self, n: int = 1) -> np.ndarray:
        """`n` standard normals, matching R's ``rnorm(n)``.

        Each draw eats two uniforms: R combines them into 2^27 levels
        because one uniform is not precise enough to invert.
        """
        u = self.unif_rand(2 * n)
        u1 = u[0::2]
        u2 = u[1::2]
        combinado = (_BIG * u1).astype(np.int64) + u2
        return _qnorm_vec(combinado / _BIG)

    # convenience aliases matching R's names
    def runif(self, n: int) -> np.ndarray:
        return self.unif_rand(n)

    def rnorm(self, n: int, mean: float = 0.0, sd: float = 1.0) -> np.ndarray:
        return mean + sd * self.norm_rand(n)

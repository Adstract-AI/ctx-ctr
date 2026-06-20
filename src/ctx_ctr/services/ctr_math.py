"""Math helpers for CTR priors and Bayesian bucket statistics."""

from __future__ import annotations

import math


def sigmoid(value: float) -> float:
    """Return the logistic transform for a real-valued weight."""

    return 1.0 / (1.0 + math.exp(-value))


def logit(probability: float) -> float:
    """Return the log-odds for a probability between zero and one."""

    if probability <= 0 or probability >= 1:
        raise ValueError("probability must be between 0 and 1")
    return math.log(probability / (1.0 - probability))


def beta_variance(alpha: float, beta: float) -> float:
    """Return the variance of a Beta distribution."""

    total = alpha + beta
    return (alpha * beta) / ((total**2) * (total + 1.0))


def clipped_confidence_interval(mean: float, variance: float, z_score: float) -> tuple[float, float]:
    """Return a clipped normal-approximation confidence interval."""

    margin = z_score * math.sqrt(variance)
    return max(0.0, mean - margin), min(1.0, mean + margin)


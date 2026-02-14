"""Hybrid search fusion algorithms (RRF and Linear)."""
from __future__ import annotations

from typing import Any
from enum import Enum


class FusionMethod(str, Enum):
    """Fusion method for combining vector and BM25 search results."""
    RRF = 'rrf'
    LINEAR = 'linear'


def reciprocal_rank_fusion(
    vector_results: list[tuple[Any, float]],
    bm25_results: list[tuple[Any, float]],
    k: int = 60,
) -> list[tuple[Any, float]]:
    """
    Reciprocal Rank Fusion (RRF) algorithm.

    Combines results from multiple search methods using rank-based scoring.
    Formula: rrf_score(doc) = sum(1 / (k + rank_i)) for each ranking

    Args:
        vector_results: List of (doc_id, distance) from vector search
                       Lower distance = better match
        bm25_results: List of (doc_id, score) from BM25 search
                     Higher score = better match
        k: Ranking constant (default: 60, standard value)

    Returns:
        List of (doc_id, rrf_score) sorted by score descending
    """
    scores: dict[Any, float] = {}

    # Process vector results (lower distance = better = lower rank)
    # Results should already be sorted by distance ascending
    for rank, (doc_id, _) in enumerate(vector_results, start=1):
        scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank)

    # Process BM25 results (higher score = better = lower rank)
    # Results should already be sorted by score descending
    for rank, (doc_id, _) in enumerate(bm25_results, start=1):
        scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank)

    # Sort by combined score descending
    sorted_results = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    return sorted_results


def linear_fusion(
    vector_results: list[tuple[Any, float]],
    bm25_results: list[tuple[Any, float]],
    vector_weight: float = 0.5,
) -> list[tuple[Any, float]]:
    """
    Weighted Linear Combination with min-max normalization.

    Normalizes both scores to [0, 1] range and combines with weights.
    Formula: final_score = α * norm(vector_sim) + (1 - α) * norm(bm25)

    Args:
        vector_results: List of (doc_id, distance) from vector search
                       Lower distance = better match
        bm25_results: List of (doc_id, score) from BM25 search
                     Higher score = better match
        vector_weight: Weight for vector similarity (0.0 to 1.0)
                      BM25 weight = 1.0 - vector_weight

    Returns:
        List of (doc_id, combined_score) sorted by score descending
    """
    if not 0.0 <= vector_weight <= 1.0:
        raise ValueError(f"vector_weight must be between 0.0 and 1.0, got {vector_weight}")

    bm25_weight = 1.0 - vector_weight

    # Convert to dicts for easier lookup
    vector_dict = {doc_id: dist for doc_id, dist in vector_results}
    bm25_dict = {doc_id: score for doc_id, score in bm25_results}

    # Get all document IDs
    all_docs = set(vector_dict.keys()) | set(bm25_dict.keys())

    if not all_docs:
        return []

    # Normalize vector distances to similarities [0, 1]
    # Lower distance = higher similarity
    if vector_dict:
        min_dist = min(vector_dict.values())
        max_dist = max(vector_dict.values())
        dist_range = max_dist - min_dist if max_dist != min_dist else 1.0

        # Convert distance to similarity: sim = 1 - (dist - min) / range
        vector_sim = {
            doc_id: 1.0 - (dist - min_dist) / dist_range
            for doc_id, dist in vector_dict.items()
        }
    else:
        vector_sim = {}

    # Normalize BM25 scores to [0, 1]
    if bm25_dict:
        min_score = min(bm25_dict.values())
        max_score = max(bm25_dict.values())
        score_range = max_score - min_score if max_score != min_score else 1.0

        bm25_norm = {
            doc_id: (score - min_score) / score_range
            for doc_id, score in bm25_dict.items()
        }
    else:
        bm25_norm = {}

    # Combine scores
    combined_scores: dict[Any, float] = {}

    for doc_id in all_docs:
        # Use 0.0 as default for missing scores (penalizes docs not in both results)
        vec_score = vector_sim.get(doc_id, 0.0)
        bm25_score = bm25_norm.get(doc_id, 0.0)

        combined_scores[doc_id] = vector_weight * vec_score + bm25_weight * bm25_score

    # Sort by combined score descending
    sorted_results = sorted(combined_scores.items(), key=lambda x: x[1], reverse=True)

    return sorted_results


def fuse_results(
    vector_results: list[tuple[Any, float]],
    bm25_results: list[tuple[Any, float]],
    method: str = FusionMethod.RRF,
    vector_weight: float = 0.5,
    rrf_k: int = 60,
) -> list[tuple[Any, float]]:
    """
    Fuse vector and BM25 search results using specified method.

    Args:
        vector_results: List of (doc_id, distance) from vector search
        bm25_results: List of (doc_id, score) from BM25 search
        method: Fusion method ('rrf' or 'linear')
        vector_weight: Weight for vector scores (only used with 'linear')
        rrf_k: K parameter for RRF (only used with 'rrf')

    Returns:
        List of (doc_id, combined_score) sorted by score descending
    """
    if method == FusionMethod.RRF or method == 'rrf':
        return reciprocal_rank_fusion(vector_results, bm25_results, k=rrf_k)
    elif method == FusionMethod.LINEAR or method == 'linear':
        return linear_fusion(vector_results, bm25_results, vector_weight=vector_weight)
    else:
        raise ValueError(f"Unknown fusion method: {method}. Use 'rrf' or 'linear'.")

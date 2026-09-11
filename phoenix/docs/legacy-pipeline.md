# Phoenix: Recommendation System

This repository contains JAX example code for the Phoenix recommendation system, which powers content ranking and retrieval. Phoenix uses transformer-based architectures for both **retrieval** (finding relevant candidates from millions of items) and **ranking** (ordering a smaller set of candidates by predicted engagement).

> **Note:** The sample transformer implementation in this repository is ported from the [Grok-1 open source release](https://github.com/xai-org/grok-1) by xAI. The core transformer architecture comes from Grok-1, adapted here for recommendation system use cases with custom input embeddings and attention masking for candidate isolation. This code is representative of the model used internally with the exception of specific scaling optimizations.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
  - [Two-Stage Recommendation Pipeline](#two-stage-recommendation-pipeline)
  - [Retrieval: Two-Tower Model](#retrieval-two-tower-model)
  - [Ranking: Transformer with Candidate Isolation](#ranking-transformer-with-candidate-isolation)
- [Key Design Decisions](#key-design-decisions)
- [Running the Code](#running-the-code)
- [License](#license)

---

## Overview

Phoenix is a recommendation system that predicts user engagement (likes, reposts, replies, etc.) for content. It operates in two stages:

1. **Retrieval**: Efficiently narrow down millions of candidates to hundreds using approximate nearest neighbor (ANN) search
2. **Ranking**: Score and order the retrieved candidates using a more expressive transformer model

---

## Architecture

### Two-Stage Recommendation Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           RECOMMENDATION PIPELINE                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   ┌──────────┐     ┌─────────────────────┐     ┌─────────────────────┐          │
│   │          │     │                     │     │                     │          │
│   │   User   │────▶│   STAGE 1:          │────▶│   STAGE 2:          │────▶ Feed│
│   │ Request  │     │   RETRIEVAL         │     │   RANKING           │          │
│   │          │     │   (Two-Tower)       │     │   (Transformer)     │          │
│   └──────────┘     │                     │     │                     │          │
│                    │   Millions → 1000s  │     │   1000s → Ranked    │          │
│                    └─────────────────────┘     └─────────────────────┘          │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

### Retrieval: Two-Tower Model

The retrieval stage uses a **two-tower architecture** that enables efficient similarity search at scale.

#### How Retrieval Works

1. **User Tower**: Encodes user features and engagement history through a transformer to produce a normalized user embedding `[B, D]`
2. **Candidate Tower**: Computes normalized embeddings for all items in the corpus `[N, D]`
3. **Similarity Search**: Retrieves top-K candidates using dot product similarity

---

### Ranking: Transformer with Candidate Isolation

The ranking model uses a transformer architecture where **candidates cannot attend to each other** during inference. This is a critical design choice that ensures the score for a candidate doesn't depend on which other candidates are in the batch


#### Ranking Model Architecture

```
                              PHOENIX RANKING MODEL
    ┌────────────────────────────────────────────────────────────────────────────┐
    │                                                                            │
    │                              OUTPUT LOGITS                                 │
    │                        [B, num_candidates, num_actions]                    │
    │                                    │                                       │
    │                                    │ Unembedding                           │
    │                                    │ Projection                            │
    │                                    │                                       │
    │                    ┌───────────────┴───────────────┐                       │
    │                    │                               │                       │
    │                    │    Extract Candidate Outputs  │                       │
    │                    │    (positions after history)  │                       │
    │                    │                               │                       │
    │                    └───────────────┬───────────────┘                       │
    │                                    │                                       │
    │                    ┌───────────────┴───────────────┐                       │
    │                    │                               │                       │
    │                    │         Transformer           │                       │
    │                    │     (with special masking)    │                       │
    │                    │                               │                       │
    │                    │   Candidates CANNOT attend    │                       │
    │                    │   to each other               │                       │
    │                    │                               │                       │
    │                    └───────────────┬───────────────┘                       │
    │                                    │                                       │
    │    ┌───────────────────────────────┼───────────────────────────────┐       │
    │    │                               │                               │       │
    │    ▼                               ▼                               ▼       │
    │ ┌──────────┐              ┌─────────────────┐              ┌────────────┐  │
    │ │   User   │              │     History     │              │ Candidates │  │
    │ │Embedding │              │   Embeddings    │              │ Embeddings │  │
    │ │  [B, 1]  │              │    [B, S, D]    │              │  [B, C, D] │  │
    │ │          │              │                 │              │            │  │
    │ │ User     │              │ Posts + Authors │              │ Posts +    │  │
    │ │ Hashes   │              │ + Actions +     │              │ Authors +  │  │
    │ │          │              │ Product Surface │              │ Product    │  │
    │ └──────────┘              └─────────────────┘              │ Surface    │  │
    │                                                            └────────────┘  │
    │                                                                            │
    └────────────────────────────────────────────────────────────────────────────┘
```

#### Attention Mask: Candidate Isolation

A key detail is the **attention mask** that prevents candidates from attending to each other while still allowing them to attend to the user and history:

```
                    ATTENTION MASK VISUALIZATION

         Keys (what we attend TO)
         ─────────────────────────────────────────────▶

         │ User │    History (S positions)    │   Candidates (C positions)    │
    ┌────┼──────┼─────────────────────────────┼───────────────────────────────┤
    │    │      │                             │                               │
    │ U  │  ✓   │  ✗   ✗   ✗   ✗   ✗   ✗   ✗  │  ✗   ✗   ✗   ✗   ✗   ✗   ✗    │
    │    │      │                             │                               │
    ├────┼──────┼─────────────────────────────┼───────────────────────────────┤
 Q  │    │      │                             │                               │
 u  │ H  │  ✓   │  ✓   ✗   ✗   ✗   ✗   ✗   ✗  │  ✗   ✗   ✗   ✗   ✗   ✗   ✗    │
 e  │ i  │  ✓   │  ✓   ✓   ✗   ✗   ✗   ✗   ✗  │  ✗   ✗   ✗   ✗   ✗   ✗   ✗    │
 r  │ s  │  ✓   │  ✓   ✓   ✓   ✗   ✗   ✗   ✗  │  ✗   ✗   ✗   ✗   ✗   ✗   ✗    │
 i  │ t  │  ✓   │  ✓   ✓   ✓   ✓   ✗   ✗   ✗  │  ✗   ✗   ✗   ✗   ✗   ✗   ✗    │
 e  │    │      │                             │                               │
 s  ├────┼──────┼─────────────────────────────┼───────────────────────────────┤
    │    │      │                             │  DIAGONAL ONLY (self-attend)  │
 │  │ C  │  ✓   │  ✓   ✓   ✓   ✓   ✓   ✓   ✓  │  ✓   ✗   ✗   ✗   ✗   ✗   ✗    │
 │  │ a  │  ✓   │  ✓   ✓   ✓   ✓   ✓   ✓   ✓  │  ✗   ✓   ✗   ✗   ✗   ✗   ✗    │
 │  │ n  │  ✓   │  ✓   ✓   ✓   ✓   ✓   ✓   ✓  │  ✗   ✗   ✓   ✗   ✗   ✗   ✗    │
 │  │ d  │  ✓   │  ✓   ✓   ✓   ✓   ✓   ✓   ✓  │  ✗   ✗   ✗   ✓   ✗   ✗   ✗    │
 │  │ i  │  ✓   │  ✓   ✓   ✓   ✓   ✓   ✓   ✓  │  ✗   ✗   ✗   ✗   ✓   ✗   ✗    │
 │  │ d  │  ✓   │  ✓   ✓   ✓   ✓   ✓   ✓   ✓  │  ✗   ✗   ✗   ✗   ✗   ✓   ✗    │
 ▼  │ s  │  ✓   │  ✓   ✓   ✓   ✓   ✓   ✓   ✓  │  ✗   ✗   ✗   ✗   ✗   ✗   ✓    │
    │    │      │                             │                               │
    └────┴──────┴─────────────────────────────┴───────────────────────────────┘

    ✓ = Can attend (1)          ✗ = Cannot attend (0)

    Legend:
    ├─ User + History: Causal attention (each position attends to itself and earlier positions)
    ├─ Candidates → User/History: Candidates CAN attend to user and all history positions
    └─ Candidates → Candidates: Candidates CANNOT attend to each other (only self)

    Note: this diagram matches the demo-path grok.py (causal mask); the production
    xrex implementation is non-causal, see the English README.
```

---

## Key Design Decisions

### 1. Hash-Based Embeddings

Both models use multiple hash functions for embedding lookup

### 2. Shared Architecture

The retrieval user tower uses the same transformer architecture as the ranking model

### 3. Multi-Action Prediction

The ranking model predicts multiple engagement types simultaneously:

```
Output: [B, num_candidates, num_actions]
                              │
                              ▼
        ┌─────────────────────────────────────┐
        │ Like │ Repost │ Reply │ Click │ ... │
        └─────────────────────────────────────┘
```

---

## Running the Code

### Installation

Install [uv](https://docs.astral.sh/uv/getting-started/installation/)

### Model Run Modes

Phoenix supports three distinct modes:

1. **Random demo weights** for validating service wiring:

   ```shell
   uv run scripts/run_grpc_gateway.py
   ```

2. **Locally trained weights** from `scripts/train_ranker.py` and `scripts/train_retrieval.py`:

   ```shell
   uv run scripts/run_grpc_gateway.py \
     --ranker-checkpoint checkpoints/model_params_step200.npz \
     --retrieval-checkpoint checkpoints/retrieval_params_step200.npz \
     --emb-tables checkpoints/embedding_tables.npz
   ```

3. **Published weights** with exported configs, split embedding tables, and retrieval corpus:

   ```shell
   git lfs pull --include="phoenix/artifacts/oss-phoenix-artifacts.zip"
   unzip artifacts/oss-phoenix-artifacts.zip -d artifacts

   uv run run_pipeline.py \
     --artifacts_dir artifacts/oss-phoenix-artifacts \
     --top_k_retrieval 200 \
     --top_k_display 30

   uv run scripts/run_grpc_gateway.py \
     --artifacts-dir artifacts/oss-phoenix-artifacts
   ```

The published offline CLI and gRPC mode both construct `services.published_pipeline.PublishedPipeline`; config/NPZ loading, hashing, preprocessing, model runners, and output mapping are shared. For reproducible post-age features, the offline entry also accepts the additive `--impression_timestamp <unix-seconds>` option; without it, the corpus end timestamp is used.

The current published LFS object is 2,903,518,802 bytes with SHA-256
`fbc6017d00588754e22e0c7eb2f786a008a74d309c03c8085fa2fad418a83dac`.
Its `config.json` files define the architecture: 128-dimensional embeddings,
4 transformer layers, 4 attention heads, 127 history positions, and 64
candidate positions. These exported configs and checkpoint shapes are the
source of truth; do not substitute architecture values from older prose docs.
The published gRPC mode loads both ranker and retrieval embedding tables, so
allow several GiB of memory.

### Running the Ranker

```shell
uv run scripts/run_ranker.py
```

### Running Retrieval

```shell
uv run scripts/run_retrieval.py
```

### Running Tests

```shell
uv run pytest
```

### Going Further

- Train your own weights: `uv run scripts/train_ranker.py` (see [docs/训练指引.md](训练指引.md))
- Serve over HTTP: `uv run scripts/run_services.py all` (see [services/README.md](../services/README.md))
- Serve over gRPC for recommendation-service: `uv run scripts/run_grpc_gateway.py`
- Run the local pure-post smoke: `../scripts/run_recommendation_demo.sh` from the repo root

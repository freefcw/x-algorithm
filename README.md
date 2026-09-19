# X For You Feed Algorithm

This repository contains a runnable port of the core recommendation system powering the "For You" feed on X. It combines in-network content (from accounts you follow) with out-of-network content (discovered through ML-based retrieval) and ranks everything using a Grok-based transformer model.

> **Note:** The transformer implementation is ported from the [Grok-1 open source release](https://github.com/xai-org/grok-1) by xAI, adapted for recommendation system use cases.

## Project Status — Read This First

X open-sourced the core algorithm, not its production infrastructure. The internal services the original system depends on (user profiles, content store, engagement logs, trust & safety) were **not** released. This repository fills those gaps with trait-based client stubs and a demo mode, so the full pipeline can actually run on your machine:

| Capability | Status |
|-----------|--------|
| Compile everything (`cargo build --workspace`, `uv sync`) | Works |
| Run ranking / retrieval model inference locally | Works (random weights out of the box) |
| Serve the models over HTTP and gRPC | Works |
| Train your own model weights (simulated or real data) | Works |
| Run the **full end-to-end pipeline** (Thunder + Phoenix + Home Mixer) and get a ranked feed | Requires production dependency integration |
| Production deployment with real data | Requires integration work — the client stubs in `home-mixer/clients/` must be pointed at your platform's services. See the [external dependency contracts](docs/home-mixer/05-external-deps-and-contracts.md) |

No pretrained weights are included. A production deployment must provide a validated checkpoint and matching retrieval index.

## Quick Start

Prerequisites: [Rust](https://rustup.rs/), `protoc` (`brew install protobuf`), [uv](https://docs.astral.sh/uv/), Kafka, Redis, and the external business adapters described in the production runbooks.

The production setup and readiness requirements live in [docs/phoenix/08-production-handbook.md](docs/phoenix/08-production-handbook.md), [docs/operations/](docs/operations/), and [docs/recommendation-production-readiness-assessment.md](docs/recommendation-production-readiness-assessment.md).

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    FOR YOU FEED REQUEST                                     │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                         HOME MIXER                                          │
│                                    (Orchestration Layer)                                    │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                   QUERY HYDRATION                                   │   │
│   │  ┌──────────────────────────┐    ┌──────────────────────────────────────────────┐   │   │
│   │  │ User Action Sequence     │    │ User Features                                │   │   │
│   │  │ (engagement history)     │    │ (following list, preferences, etc.)          │   │   │
│   │  └──────────────────────────┘    └──────────────────────────────────────────────┘   │   │
│   └─────────────────────────────────────────────────────────────────────────────────────┘   │
│                                              │                                              │
│                                              ▼                                              │
│   ┌─────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                  CANDIDATE SOURCES                                  │   │
│   │         ┌─────────────────────────────┐    ┌────────────────────────────────┐       │   │
│   │         │        THUNDER              │    │     PHOENIX RETRIEVAL          │       │   │
│   │         │    (In-Network Posts)       │    │   (Out-of-Network Posts)       │       │   │
│   │         │                             │    │                                │       │   │
│   │         │  Posts from accounts        │    │  ML-based similarity search    │       │   │
│   │         │  you follow                 │    │  across global corpus          │       │   │
│   │         └─────────────────────────────┘    └────────────────────────────────┘       │   │
│   └─────────────────────────────────────────────────────────────────────────────────────┘   │
│                                              │                                              │
│                                              ▼                                              │
│   ┌─────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                      HYDRATION                                      │   │
│   │  Fetch additional data: core post metadata, author info, media entities, etc.       │   │
│   └─────────────────────────────────────────────────────────────────────────────────────┘   │
│                                              │                                              │
│                                              ▼                                              │
│   ┌─────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                      FILTERING                                      │   │
│   │  Remove: duplicates, old posts, self-posts, blocked authors, muted keywords, etc.   │   │
│   └─────────────────────────────────────────────────────────────────────────────────────┘   │
│                                              │                                              │
│                                              ▼                                              │
│   ┌─────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                       SCORING                                       │   │
│   │  ┌──────────────────────────┐                                                       │   │
│   │  │  Phoenix Scorer          │    Grok-based Transformer predicts:                   │   │
│   │  │  (ML Predictions)        │    P(like), P(reply), P(repost), P(click)...          │   │
│   │  └──────────────────────────┘                                                       │   │
│   │               │                                                                     │   │
│   │               ▼                                                                     │   │
│   │  ┌──────────────────────────┐                                                       │   │
│   │  │  Ranking Scorer          │    Weighted Score = Σ (weight × P(action))            │   │
│   │  │  (one component)         │    then author diversity and OON attenuation          │   │
│   │  └──────────────────────────┘                                                       │   │
│   └─────────────────────────────────────────────────────────────────────────────────────┘   │
│                                              │                                              │
│                                              ▼                                              │
│   ┌─────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                      SELECTION                                      │   │
│   │                    Sort by final score, select top K candidates                     │   │
│   └─────────────────────────────────────────────────────────────────────────────────────┘   │
│                                              │                                              │
│                                              ▼                                              │
│   ┌─────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                              FILTERING (Post-Selection)                             │   │
│   │                 Visibility filtering (deleted/spam/violence/gore etc)               │   │
│   └─────────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                             │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                     RANKED FEED RESPONSE                                    │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Components

### Home Mixer

**Location:** [`home-mixer/`](home-mixer/)

The orchestration layer that assembles the For You feed. It leverages the `CandidatePipeline` framework with the following stages:

| Stage | Description |
|-------|-------------|
| Query Hydrators | Fetch user context (engagement history, following list) |
| Sources | Retrieve candidates from Thunder and Phoenix |
| Hydrators | Enrich candidates with additional data |
| Filters | Remove ineligible candidates |
| Scorers | Predict engagement and compute final scores |
| Selector | Sort by score and select top K |
| Post-Selection Filters | Final visibility and dedup checks |
| Side Effects | Cache request info for future use |

The server exposes `ScoredPostsService` (ranked posts) and `ForYouFeedService` (final feed). Upstream dependencies (user profiles, post content, engagement logs, trust & safety) are abstracted behind traits in `home-mixer/clients/`; production deployment requires wiring those adapters to the corresponding business services.

### Thunder

**Location:** [`thunder/`](thunder/)

An in-memory post store and realtime ingestion pipeline that tracks recent posts from all users. It:

- Consumes post create/delete events from Kafka
- Maintains per-user stores for original posts, replies/reposts, and video posts
- Serves "in-network" post candidates from accounts the requesting user follows
- Automatically trims posts older than the retention period

Thunder enables sub-millisecond lookups for in-network content without hitting an external database.

### Phoenix

**Location:** [`phoenix/`](phoenix/)

The ML component (Python ≥ 3.11 / JAX) with two main functions:

#### 1. Retrieval (Two-Tower Model)
Finds relevant out-of-network posts:
- **User Tower**: Encodes user features and engagement history into an embedding
- **Candidate Tower**: Encodes all posts into embeddings
- **Similarity Search**: Retrieves top-K posts via dot product similarity

#### 2. Ranking (Transformer with Candidate Isolation)
Predicts engagement probabilities for each candidate:
- Takes user context (engagement history) and candidate posts as input
- Uses special attention masking so candidates cannot attend to each other
- Outputs probabilities for each action type (like, reply, repost, click, etc.)

Phoenix ships the xrex production training and inference engine. Ranking and retrieval are separate gRPC services launched with `phoenix/xrex/inference/launch_inference.py`; the xrex contract is not yet an adapter for Home Mixer's former demo gateway contract. See [`phoenix/README.md`](phoenix/README.md).

### Candidate Pipeline

**Location:** [`candidate-pipeline/`](candidate-pipeline/)

A reusable framework for building recommendation pipelines. Defines traits for:

| Trait | Purpose |
|-------|---------|
| `Source` | Fetch candidates from a data source |
| `Hydrator` | Enrich candidates with additional features |
| `Filter` | Remove candidates that shouldn't be shown |
| `Scorer` | Compute scores for ranking |
| `Selector` | Sort and select top candidates |
| `SideEffect` | Run async side effects (caching, logging) |

The framework runs sources and hydrators in parallel where possible, with configurable error handling and logging.

## How It Works

### Pipeline Stages

1. **Query Hydration**: Fetch the user's recent engagement history and metadata (eg. following list)

2. **Candidate Sourcing**: Retrieve candidates from:
   - **Thunder**: Recent posts from followed accounts (in-network)
   - **Phoenix Retrieval**: ML-discovered posts from the global corpus (out-of-network)

3. **Candidate Hydration**: Enrich candidates with:
   - Core post data (text, media, etc.)
   - Author information (username, verification status)
   - Video duration (for video posts)
   - Subscription status

4. **Pre-Scoring Filters**: Remove posts that are:
   - Duplicates
   - Too old
   - From the viewer themselves
   - From blocked/muted accounts
   - Containing muted keywords
   - Previously seen or recently served
   - Ineligible subscription content

5. **Scoring**: Apply scorers sequentially:
   - **Phoenix Scorer**: Get ML predictions from the Phoenix transformer model
   - **Ranking Scorer**: Combine predictions into a weighted score, then apply author diversity and out-of-network attenuation
   - Optional: **VM Ranker** (second-pass rerank) and **Author Cold Start** (behind explicit flags)

6. **Selection**: Sort by score and select the top K candidates

7. **Post-Selection Processing**: Final validation of post candidates to be served

### Scoring and Ranking

The Phoenix Grok-based transformer model predicts probabilities for multiple engagement types:

```
Predictions:
├── P(favorite)
├── P(reply)
├── P(repost)
├── P(quote)
├── P(click)
├── P(profile_click)
├── P(video_quality_view)  (vqv_score in code)
├── P(photo_expand)
├── P(share)
├── P(dwell)
├── P(follow_author)
├── P(not_interested)
├── P(block_author)
├── P(mute_author)
└── P(report)  (+ share_via_dm, share_via_copy_link, quoted_click; demo model: 18 discrete heads + continuous dwell_time)
```

The **Ranking Scorer** combines these into a final score:

```
Final Score = Σ (weight_i × P(action_i))
```

Positive actions (like, repost, share) have positive weights. Negative actions (block, mute, report) have negative weights, pushing down content the user would likely dislike. The weights live in [`home-mixer/params/`](home-mixer/params/) — they are open-source defaults mirrored from upstream, not X's live production values.

### Filtering

Filters run at two stages:

**Pre-Scoring Filters:**
| Filter | Purpose |
|--------|---------|
| `DropDuplicatesFilter` | Remove duplicate post IDs |
| `CoreDataHydrationFilter` | Remove posts that failed to hydrate core metadata |
| `FirstStageEligibleFilter` | Remove posts the business first-stage check marked ineligible (deleted / not public / failed audit) |
| `AgeFilter` | Remove posts older than threshold (`created_at_ms`, falling back to the ObjectId timestamp) |
| `SelfTweetFilter` | Remove user's own posts |
| `PreviouslySeenPostsFilter` | Remove posts user has already seen |
| `PreviouslySeenPostsBackupFilter` | Backup seen-id filter when the request only has impression IDs |
| `PreviouslyServedPostsFilter` | Remove posts already served in session |
| `ViewerMutedKeywordFilter` | Remove posts whose main or quoted text hits the viewer's muted keywords |
| `AuthorSocialgraphFilter` | Remove posts from blocked/muted authors |
| `VideoFilter` | Drop video posts when the request sets `exclude_videos` |
| `TopicIdsFilter` / `NewUserTopicIdsFilter` | Keep topic-constrained requests on-topic |

**Post-Selection Filters:**
| Filter | Purpose |
|--------|---------|
| `VFFilter` | Remove posts that are deleted/spam/violence/gore etc.; unverified posts are dropped by default (`HOME_MIXER_VF_FAILURE_POLICY=fail_closed`) |
| `DedupConversationFilter` | Deduplicate multiple branches of the same conversation thread |

Quote / repost / subscription specific components (`RetweetDeduplicationFilter`, `IneligibleSubscriptionFilter`, `AncillaryVFFilter`, `QuoteHydrator`, `SubscriptionHydrator`) were removed because the target product has no such concepts; the shared fields stay empty.

## Key Design Decisions

### 1. No Hand-Engineered Features
The system relies entirely on the Grok-based transformer to learn relevance from user engagement sequences. No manual feature engineering for content relevance. This significantly reduces the complexity in data pipelines and serving infrastructure.

### 2. Candidate Isolation in Ranking
During transformer inference, candidates cannot attend to each other—only to the user context. This ensures the score for a post doesn't depend on which other posts are in the batch, making scores consistent and cacheable.

### 3. Hash-Based Embeddings
Both retrieval and ranking use multiple hash functions for embedding lookup.

### 4. Multi-Action Prediction
Rather than predicting a single "relevance" score, the model predicts probabilities for many actions.

### 5. Composable Pipeline Architecture
The `candidate-pipeline` crate provides a flexible framework for building recommendation pipelines with:
- Separation of pipeline execution and monitoring from business logic
- Parallel execution of independent stages and graceful error handling
- Easy addition of new sources, hydrations, filters, and scorers

## Documentation

- **[docs/bootstrap/](docs/bootstrap/)** — comprehensive bootstrap runbook: environment, builds, models, end-to-end demo, real data, capability gates, and production readiness (Chinese)
- **[docs/getting-started/](docs/getting-started/)** — step-by-step: environment → model demo → serving → training → full pipeline → production gaps (Chinese)
- **[docs/README.md](docs/README.md)** — documentation hub with per-module deep dives
- **[README_zh.md](README_zh.md)** — this document in Chinese

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

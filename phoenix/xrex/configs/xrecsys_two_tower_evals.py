# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 X.AI Corp.
from xrex.configs import config_registry
from xrex.data.retrieval_dataset import RetrievalDataset
from xrex.eval.eval_utils import EvalModule
from xrex.eval.recsys_eval import RecsysTwoTowerEval
from xrex.models.recsys_two_tower_model import RecsysTwoTowerModelConfig


def build_evals(mparams, dataset, has_user_features_token) -> list[EvalModule]:
    raw_target_dataset_type = mparams.get("target_dataset_type", RetrievalDataset.HOME)
    if isinstance(raw_target_dataset_type, str):
        target_dataset_type = RetrievalDataset[raw_target_dataset_type]
    else:
        target_dataset_type = raw_target_dataset_type

    positive_actions = mparams.get(
        "positive_actions", RecsysTwoTowerModelConfig.get_positive_actions()
    )
    soft_negative_actions = mparams.get(
        "soft_negative_actions", RecsysTwoTowerModelConfig.get_soft_negative_actions()
    )
    hard_negative_actions = mparams.get(
        "hard_negative_actions", RecsysTwoTowerModelConfig.get_hard_negative_actions()
    )

    recsys_two_tower_eval = RecsysTwoTowerEval(
        max_steps=1,
        positive_actions=positive_actions,
        implicit_negative_actions=soft_negative_actions,
        explicit_negative_actions=hard_negative_actions,
        candidate_seq_len=mparams["candidate_seq_len"],
        target_dataset_type=target_dataset_type,
    )
    if mparams.get("num_candidate_heads", 1) > 1:
        eval_bs = mparams.get("eval_bs_per_device", 32)
    else:
        eval_bs = mparams.get("eval_bs_per_device", mparams["bs_per_device"])
    evals = []
    evals.append(
        EvalModule(
            eval_name="recsys_two_tower_eval",
            eval_conf=recsys_two_tower_eval,
            eval_bs_per_device=eval_bs,
            eval_seq_len=(
                int(mparams.get("use_user_embedding", True))
                + int(has_user_features_token(mparams))
                + mparams["history_seq_len"]
            ),
            eval_dataset=dataset,
        )
    )

    if mparams.get("num_candidate_heads", 1) > 1:
        immersive_positive_actions = mparams.get("immersive_positive_actions", positive_actions)
        immersive_eval = RecsysTwoTowerEval(
            max_steps=1,
            positive_actions=immersive_positive_actions,
            implicit_negative_actions=soft_negative_actions,
            explicit_negative_actions=hard_negative_actions,
            candidate_seq_len=mparams["candidate_seq_len"],
            target_dataset_type=RetrievalDataset.IMMERSIVE2Day,
        )
        evals.append(
            EvalModule(
                eval_name="recsys_two_tower_immersive_eval",
                eval_conf=immersive_eval,
                eval_bs_per_device=eval_bs,
                eval_seq_len=1 + mparams["history_seq_len"],
                eval_dataset=dataset,
            )
        )

    return evals


def make_wandb_hook(evaluator, ctx):
    if not evaluator.enable_wandb:
        return None
    from xrex.driver.hooks import WandbHook

    wandb_hook = WandbHook(evaluator.wandb_project, evaluator.wandb_entity)
    temp_h2d_state = ctx.config.h2d_state
    ctx.config.h2d_state = None
    wandb_hook.on_boot(ctx.run, ctx.run_dir, ctx.config)
    ctx.config.h2d_state = temp_h2d_state
    return wandb_hook


config_registry.RETRIEVAL_EVAL_BUILDERS.append(build_evals)
config_registry.EVAL_WANDB_HOOK_FACTORIES.append(make_wandb_hook)

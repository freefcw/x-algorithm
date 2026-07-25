from runners import ACTIONS
from run_pipeline import IDX_DWELL, IDX_FAV, IDX_REPLY, IDX_RT, IDX_VQV


def test_offline_pipeline_indices_follow_model_action_order():
    """The offline ranker must index logits using the model action contract."""
    assert {
        "favorite_score": IDX_FAV,
        "reply_score": IDX_REPLY,
        "repost_score": IDX_RT,
        "dwell_score": IDX_DWELL,
        "vqv_score": IDX_VQV,
    } == {
        action_name: ACTIONS.index(action_name)
        for action_name in (
            "favorite_score",
            "reply_score",
            "repost_score",
            "dwell_score",
            "vqv_score",
        )
    }

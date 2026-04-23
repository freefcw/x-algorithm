#!/usr/bin/env python3
"""
Phoenix API 服务客户端示例

演示如何调用精排和召回服务。
"""

import requests

BASE_URL = "http://localhost:8080"


def check_health():
    """检查服务健康状态"""
    resp = requests.get(f"{BASE_URL}/health")
    print("Health Check:", resp.json())
    return resp.json()


def call_rank(user_id: str = "user_123"):
    """调用精排接口"""
    payload = {
        "user_id": user_id,
        "history_len": 32,
        "num_candidates": 8,
    }
    resp = requests.post(f"{BASE_URL}/v1/rank", json=payload)
    data = resp.json()
    
    print(f"\n精排结果 (User: {data['user_id']}):")
    print(f"Total candidates: {data['total_candidates']}")
    print("\n排序结果 (按点赞概率降序):")
    print(f"{'Rank':<6} {'Cand':<8} {'Favorite':<10} {'Reply':<10} {'Repost':<10}")
    print("-" * 50)
    for r in data['ranked_results'][:5]:  # 只显示前5个
        print(f"{r['candidate_idx']:<6} {r['candidate_idx']:<8} "
              f"{r['favorite_prob']:.4f}    {r['reply_prob']:.4f}    {r['repost_prob']:.4f}")


def call_retrieve(user_id: str = "user_456", top_k: int = 10):
    """调用召回接口"""
    payload = {
        "user_id": user_id,
        "history_len": 32,
        "top_k": top_k,
    }
    resp = requests.post(f"{BASE_URL}/v1/retrieve", json=payload)
    data = resp.json()
    
    print(f"\n召回结果 (User: {data['user_id']}, Top-K: {data['top_k']}):")
    print(f"{'Rank':<6} {'Post ID':<12} {'Similarity':<12}")
    print("-" * 35)
    for r in data['results']:
        bar = "█" * int((r['similarity'] + 1) * 10) + "░" * (20 - int((r['similarity'] + 1) * 10))
        print(f"{r['rank']:<6} {r['post_id']:<12} {bar} {r['similarity']:.4f}")


if __name__ == "__main__":
    import sys
    
    # 检查服务是否就绪
    try:
        health = check_health()
        if not health.get("ranker_ready") or not health.get("retrieval_ready"):
            print("服务未就绪，请检查服务器状态")
            sys.exit(1)
    except requests.exceptions.ConnectionError:
        print(f"无法连接到服务，请确保服务已启动: uvicorn api_server:app --host 0.0.0.0 --port 8080")
        sys.exit(1)
    
    # 调用示例
    print("\n" + "=" * 60)
    call_rank("user_demo_1")
    
    print("\n" + "=" * 60)
    call_retrieve("user_demo_2", top_k=10)
    
    print("\n" + "=" * 60)
    print("客户端演示完成!")

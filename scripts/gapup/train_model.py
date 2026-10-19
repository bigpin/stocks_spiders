#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
训练 gapup 模型（一次训练产出两个模型）：
1. gapup_daily_model.pkl —— 仅日线特征（晚间一级筛选用：全市场排序选次日候选池）
2. gapup_model.pkl      —— 全特征（盘中14:30用：从候选池精选 top N）

- 时序切分（防止未来信息泄漏）：train < val_start <= val < test_start <= test
- 模型：LogisticRegression 基线 vs HistGradientBoostingClassifier，各自选优
- 指标：AUC、Precision@3/@5（按日期分组取当日 top-K 命中率）、top20% 命中提升
- 训练样本默认限定 passes_screener=True（与部署分布对齐），--all-samples 关闭

用法：
  python scripts/gapup/train_model.py
  python scripts/gapup/train_model.py --val-start 2026-06-01 --test-start 2026-09-01
"""

import argparse
import os
import pickle
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common
from common import CONFIG, FEATURE_COLUMNS, MODELS_DIR
from build_dataset import DAILY_FEATURE_COLUMNS

logger = common.get_gapup_logger()

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler


def precision_at_k(df, prob_col, k_list=(3, 5)):
    """按日期分组取 top-K 的命中率。返回 {k: (命中率, 覆盖天数)}。"""
    out = {}
    for k in k_list:
        hits, days = [], 0
        for _, day in df.groupby('date'):
            if len(day) == 0:
                continue
            top = day.nlargest(min(k, len(day)), prob_col)
            hits.append(top['is_high_open'].mean())
            days += 1
        out[k] = (float(np.mean(hits)) if hits else float('nan'), days)
    return out


def evaluate(name, df, prob_col):
    if df.empty:
        logger.info(f"  [{name}] 无样本")
        return None
    base = df['is_high_open'].mean()
    if base <= 0:
        logger.info(f"  [{name}] n={len(df)} 无正样本")
        return None
    auc = roc_auc_score(df['is_high_open'], df[prob_col])
    thr = df[prob_col].quantile(0.8)
    top_rate = df[df[prob_col] >= thr]['is_high_open'].mean()
    logger.info(f"  [{name}] n={len(df)} 高开率={base * 100:.1f}% AUC={auc:.4f} "
                f"top20%命中={top_rate * 100:.1f}% (lift {top_rate / base:.2f}x)")
    pk = precision_at_k(df, prob_col)
    parts = [f"P@{k}={v[0] * 100:.0f}%({v[1]}天)" for k, v in pk.items()]
    logger.info(f"    按日topK: {' '.join(parts)}")
    return auc


def calibration_table(df, prob_col, min_n=30):
    """分箱校准表：模型说 vs 实际发生率。返回 DataFrame 或 None。"""
    bins = [0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1.01]
    labels = ['0-10%', '10-20%', '20-30%', '30-40%', '40-50%',
              '50-60%', '60-70%', '70-80%', '80-90%', '90-100%']
    d = df.assign(_bin=pd.cut(df[prob_col], bins=bins, labels=labels))
    t = d.groupby('_bin', observed=True).agg(
        n=('is_high_open', 'size'), 模型说=(prob_col, 'mean'), 实际=('is_high_open', 'mean'))
    t = t[t['n'] >= min_n]
    if t.empty:
        return None
    t['模型说'] = (t['模型说'] * 100).round(1)
    t['实际'] = (t['实际'] * 100).round(1)
    return t


def make_models():
    return {
        'logreg': make_pipeline(
            SimpleImputer(strategy='median'), StandardScaler(),
            LogisticRegression(max_iter=2000, C=0.5)),
        'histgb': HistGradientBoostingClassifier(
            max_iter=300, learning_rate=0.06, max_depth=6,
            min_samples_leaf=40, l2_regularization=1.0, random_state=42),
    }


def train_variant(tag, feats, tr, va, te, save_name):
    """训练一个模型变体（日线/全量特征），选优落盘。"""
    logger.info(f"\n===== {tag}（{len(feats)} 特征）=====")
    Xtr, ytr = tr[feats], tr['is_high_open'].values
    best_name, best_auc, best_model = None, -1, None
    for name, model in make_models().items():
        logger.info(f"-- 模型 {name}")
        model.fit(Xtr, ytr)
        for split_name, split in (('验证', va), ('测试', te)):
            if split.empty:
                continue
            prob = model.predict_proba(split[feats])[:, 1]
            auc = evaluate(split_name, split.assign(prob=prob), 'prob')
            if split_name == '验证' and auc is not None and auc > best_auc:
                best_name, best_auc, best_model = name, auc, model
    if best_model is None:
        logger.warning(f"{tag}: 没有可用模型，跳过保存")
        return
    logger.info(f"{tag} 选优: {best_name} (验证AUC={best_auc:.4f})")

    # 概率校准（等渗回归）：用验证集拟合「模型原始概率 -> 实际发生率」映射，
    # 修正树模型在高档位的系统性过度自信（如说 80% 实际 ~60%）
    calibrator = None
    if not va.empty:
        val_prob = best_model.predict_proba(va[feats])[:, 1]
        calibrator = IsotonicRegression(out_of_bounds='clip')
        calibrator.fit(val_prob, va['is_high_open'].values)
        if not te.empty:
            te_prob = calibrator.predict(best_model.predict_proba(te[feats])[:, 1])
            te_cal = te.assign(prob_cal=te_prob)
            logger.info(f"{tag} 校准后（测试集，按校准概率分箱）:")
            ct = calibration_table(te_cal, 'prob_cal')
            if ct is not None:
                for idx, row in ct.iterrows():
                    logger.info(f"    {idx}: n={row['n']} 模型说={row['模型说']}% 实际={row['实际']}%")

    os.makedirs(MODELS_DIR, exist_ok=True)
    path = os.path.join(MODELS_DIR, save_name)
    with open(path, 'wb') as f:
        pickle.dump({
            'model': best_model,
            'features': feats,
            'calibrator': calibrator,
            'config': CONFIG,
            'val_auc': best_auc,
            'train_rows': len(tr),
            'trained_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        }, f)
    logger.info(f"已保存: {path}")


def main():
    parser = argparse.ArgumentParser(description='训练 gapup 模型（日线+全量）')
    parser.add_argument('--data', default=os.path.join(common.DATA_DIR, 'training_samples.parquet'))
    parser.add_argument('--val-start', default='2026-06-01')
    parser.add_argument('--test-start', default='2026-09-01')
    parser.add_argument('--all-samples', action='store_true',
                        help='不过滤 passes_screener（默认只训练过筛样本，对齐部署分布）')
    parser.add_argument('--no-save', action='store_true')
    args = parser.parse_args()

    df = pd.read_parquet(args.data)
    df['date'] = df['date'].astype(str)
    if not args.all_samples and 'passes_screener' in df.columns:
        before = len(df)
        df = df[df['passes_screener']]
        logger.info(f"按 passes_screener 过滤: {before} -> {len(df)} 行")
    feats_full = [c for c in FEATURE_COLUMNS if c in df.columns]
    feats_daily = [c for c in DAILY_FEATURE_COLUMNS if c in df.columns]
    logger.info(f"样本 {len(df)} 行，切分: "
                f"train<{args.val_start} / val {args.val_start}~{args.test_start} / test>={args.test_start}")

    tr = df[df['date'] < args.val_start]
    va = df[(df['date'] >= args.val_start) & (df['date'] < args.test_start)]
    te = df[df['date'] >= args.test_start]
    if tr.empty or va.empty:
        logger.error("训练/验证集为空，请检查数据日期范围或切分参数")
        return

    if not args.no_save:
        train_variant('日线模型（晚间筛选用）', feats_daily, tr, va, te, 'gapup_daily_model.pkl')
    train_variant('全量模型（盘中精选用）', feats_full, tr, va, te, 'gapup_model.pkl')


if __name__ == '__main__':
    main()

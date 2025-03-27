# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import multiprocessing as mp
import re
from enum import Enum
from pathlib import Path
from typing import Any, List, Tuple

import pandas as pd
import psutil
from hta.configs.config import logger
from hta.utils.utils import shorten_name,get_kernel_type

def merge_intervals(intervals):
    """
    合并重叠的区间
    :param intervals: 列表，每个元素是一个区间 [ts, te]
    :return: 合并后的区间列表
    """
    if not intervals:
        return []

    # 按 ts 排序
    intervals.sort(key=lambda x: x[0])

    merged = []
    current_interval = intervals[0]

    for interval in intervals[1:]:
        ts, te = interval
        # 如果当前区间与下一个区间重叠，合并它们
        if ts <= current_interval[1]:
            current_interval[1] = max(current_interval[1], te)
        else:
            # 如果不重叠，将当前区间加入结果，并更新当前区间
            merged.append(current_interval)
            current_interval = interval

    # 加入最后一个区间
    merged.append(current_interval)

    return merged

def calculate_non_overlap_length(ts, te, merged_intervals):
    """
    计算区间 [ts, te] 与合并后的区间列表的不交叠长度
    :param ts: 区间开始时间
    :param te: 区间结束时间
    :param merged_intervals: 合并后的区间列表，每个元素是 [ts, te]
    :return: 不交叠的长度
    """
    # 初始化不交叠的长度为整个区间的长度
    non_overlap_length = te - ts

    # 遍历合并后的区间列表
    for merged_ts, merged_te in merged_intervals:
        # 计算交叠部分
        overlap_start = max(ts, merged_ts)
        overlap_end = min(te, merged_te)
        overlap_length = max(0, overlap_end - overlap_start)

        # 减去交叠部分的长度
        non_overlap_length -= overlap_length

    # 确保不交叠长度非负
    return max(0, non_overlap_length)

def calculate_overlap_intervals(ts, te, merged_intervals):
    """
    计算 [ts, te] 与 merged_intervals 中所有区间的交叠结果
    :param ts: 区间开始时间
    :param te: 区间结束时间
    :param merged_intervals: 合并后的区间列表，格式为 [[start1, end1], [start2, end2], ...]
    :return: 交叠区间的列表
    """
    overlap_intervals = []
    
    # 遍历 merged_intervals 中的每个区间
    for interval in merged_intervals:
        start, end = interval
        
        # 计算交叠部分
        overlap_start = max(ts, start)
        overlap_end = min(te, end)
        
        # 如果存在交叠部分，则添加到结果列表中
        if overlap_start < overlap_end:
            overlap_intervals.append([overlap_start, overlap_end])
    
    return overlap_intervals



def filter_and_process_data(
    df, 
    cat_name_filter="user_annotation", 
    search_pattern="forward", 
    groupby_column="name_name"
):
    """
    筛选数据并处理的函数
    :param df: 输入的 DataFrame
    :param cat_name_filter: 用于筛选 cat_name 列的关键词（默认："user_annotation"）
    :param search_pattern: 用于模糊搜索的关键词（默认："forward"）
    :param groupby_column: 分组列名（默认："name_name"）
    :return: 
        - result_df: 筛选后的中间结果 DataFrame
        - processed_df: 处理后的 DataFrame
    """
    # 筛选出所有数据类型为 object（字符串）的列
    str_columns = df.select_dtypes(include=['object']).columns
    
    # 筛选 cat_name 列中包含指定关键词的行
    user_annotation_mask = df['cat_name'].str.contains(cat_name_filter, case=False, na=False)
    
    # 确保这些列中的数据是字符串类型
    df[str_columns] = df[str_columns].astype(str)
    
    # 在这些列中查找包含指定关键词的行（模糊搜索）
    search_mask = df[str_columns].apply(lambda col: col.str.contains(search_pattern, case=False, na=False)).any(axis=1)
    
    # 结合两个条件，筛选出最终结果
    result_df = df[user_annotation_mask & search_mask]
    
    # 按指定列分组并处理
    processed_df = result_df.groupby(groupby_column, group_keys=False).apply(
        lambda group: pd.Series({
            'total_dur': sum(te - ts for ts, te in merge_intervals([[row['ts'], row['ts'] + row['dur']] for _, row in group.iterrows()])),
            'merged_intervals': merge_intervals([[row['ts'], row['ts'] + row['dur']] for _, row in group.iterrows()])
        })
    ).reset_index()
    
    return result_df, processed_df


def merge_all_overlap_intervals(df, overlap_intervals_col='overlap_intervals'):
    """
    将 DataFrame 中某一列的所有区间合并为互不交叠的区间列表
    :param df: 输入的 DataFrame
    :param overlap_intervals_col: 存储区间的列名（默认：'overlap_intervals'）
    :return: 合并后的区间列表
    """
    # 提取所有区间
    all_intervals = []
    for intervals in df[overlap_intervals_col]:
        all_intervals.extend(intervals)
    
    # 合并重叠区间
    merged_intervals = merge_intervals(all_intervals)
    
    return merged_intervals

def calculate_total_interval_length(merged_intervals):
    """
    计算合并后的区间列表的总长度
    :param merged_intervals: 合并后的区间列表，格式为 [[start1, end1], [start2, end2], ...]
    :return: 总长度
    """
    total_length = 0
    for interval in merged_intervals:
        start, end = interval
        total_length += end - start
    return total_length

def calculate_overlap_ratio(df, merged_intervals, overlap_intervals_col='overlap_intervals'):
    """
    计算交叠时间总时长与 merged_intervals 总时长的占比
    :param df: 包含 overlap_intervals_col 列的 DataFrame
    :param merged_intervals: 合并后的区间列表，格式为 [[start1, end1], [start2, end2], ...]
    :param overlap_intervals_col: 交叠区间列名（默认：'overlap_intervals_col'）
    :return: 占比（百分比）
    """
    # 检查 df 是否包含所需的列
    if  overlap_intervals_col not in df.columns:
        raise ValueError(f"DataFrame 必须包含 {overlap_intervals_col} 列")

    # 所有交叠的区间
    total_merged_intervals = merge_all_overlap_intervals(df, overlap_intervals_col=overlap_intervals_col)
    total_overlap_length = calculate_total_interval_length(total_merged_intervals)
    
    # 计算 merged_intervals 的总时长
    total_merged_intervals_time = sum(end - start for start, end in merged_intervals)
    # print(total_overlap_length, total_merged_intervals_time)
    
    # 计算占比
    if total_merged_intervals_time == 0:
        return 0  # 避免除零错误
    overlap_ratio = (total_overlap_length / total_merged_intervals_time) * 100
    return overlap_ratio


def intersect_intervals(A1, A2):
    """
    对两个区间列表取交集
    :param A1: 区间列表 A1，格式为 [[start1, end1], [start2, end2], ...]
    :param A2: 区间列表 A2，格式为 [[start1, end1], [start2, end2], ...]
    :return: 交集后的区间列表
    """
    intersection = []
    
    # 遍历 A1 和 A2 的所有区间对
    for interval1 in A1:
        for interval2 in A2:
            start1, end1 = interval1
            start2, end2 = interval2
            
            # 计算交集
            intersect_start = max(start1, start2)
            intersect_end = min(end1, end2)
            
            # 如果存在交集，则添加到结果中
            if intersect_start < intersect_end:
                intersection.append([intersect_start, intersect_end])
    
    # 合并重叠的区间
    return merge_intervals(intersection)


def subtract_intervals(A1, A2):
    """
    计算 A1 中不与 A2 交叠的部分
    :param A1: 区间列表 A1，格式为 [[start1, end1], [start2, end2], ...]
    :param A2: 区间列表 A2，格式为 [[start1, end1], [start2, end2], ...]
    :return: A1 中不与 A2 交叠的区间列表
    """
    # 计算 A1 和 A2 的交集
    intersection = intersect_intervals(A1, A2)
    
    # 如果没有交集，直接返回 A1
    if not intersection:
        return A1
    
    # 从 A1 中去除交集部分
    result = []
    for interval in A1:
        start, end = interval
        remaining = [[start, end]]
        
        # 逐个减去交集部分
        for intersect_start, intersect_end in intersection:
            new_remaining = []
            for r_start, r_end in remaining:
                # 如果当前区间与交集区间没有重叠，直接保留
                if r_end <= intersect_start or r_start >= intersect_end:
                    new_remaining.append([r_start, r_end])
                else:
                    # 否则，将区间分割为不重叠的部分
                    if r_start < intersect_start:
                        new_remaining.append([r_start, intersect_start])
                    if r_end > intersect_end:
                        new_remaining.append([intersect_end, r_end])
            remaining = new_remaining
        
        # 将剩余部分添加到结果中
        result.extend(remaining)
    
    # 合并重叠的区间
    return merge_intervals(result)

def add_non_overlap_length_to_df(df, ts_col, dur_col, merged_intervals):
    """
    将不交叠长度和交叠区间添加到 DataFrame 中
    :param df: 输入的 DataFrame
    :param ts_col: 时间戳列名
    :param dur_col: 持续时间列名
    :param merged_intervals: 合并后的区间列表
    :return: 添加了不交叠长度和交叠区间列的 DataFrame
    """
    non_overlap_lengths = []
    overlap_intervals_list = []
    
    # 遍历 DataFrame 中的每一行
    for _, row in df.iterrows():
        ts = row[ts_col]
        te = ts + row[dur_col]
        
        # 计算不交叠长度
        non_overlap_length = calculate_non_overlap_length(ts, te, merged_intervals)
        non_overlap_lengths.append(non_overlap_length)
        
        # 计算交叠区间
        overlap_intervals = calculate_overlap_intervals(ts, te, merged_intervals)
        overlap_intervals_list.append(overlap_intervals)
    
    # 将不交叠长度和交叠区间添加到 DataFrame 中，使用 .loc 避免警告
    df.loc[:, 'non_overlap_length'] = non_overlap_lengths
    # df.loc[:, 'overlap_intervals'] = overlap_intervals_list
    # 将交叠区间添加到 DataFrame 中，存储为 object 类型
    df['overlap_intervals'] = overlap_intervals_list
    
    return df

def analyze_compute_stage(df, search_pattern="forward"):

    # 筛选用来分析的数据
    # 筛选 COMPUTATION 数据，且 stream 不为 -1，gpu kernal 层面 
    comp_df = df[(df['kernel_type'] == 'COMPUTATION') & (df['stream'] != -1)]
    non_comp_df = df[(df['kernel_type'] != 'COMPUTATION') & (df['stream'] != -1)]

    # 在原dataframe中筛选出人为打标 forward 的数据
    filtered_df, result_df = filter_and_process_data(
        df, 
        cat_name_filter="user_annotation", 
        search_pattern=search_pattern, # 传backward也行
        groupby_column="name_name"
        )
    
    unique_names_name = set(result_df['name_name'])

    for target_name in unique_names_name:
        # 对于指定的 阶段 合并出其实际耗时区间
        # merged_intervals = get_merged_intervals(result_df, groupby_column="name_name", target_name=target_name)
        merged_intervals = result_df[result_df["name_name"] == target_name]["merged_intervals"].values[0]
        total_merged_intervals_time = sum(end - start for start, end in merged_intervals)
        result_df.loc[result_df['name_name'] == target_name, 'total_dur'] = total_merged_intervals_time
        
        non_comp_df = add_non_overlap_length_to_df(non_comp_df, ts_col='ts', dur_col='dur', merged_intervals=merged_intervals)
        overlap_ratio_noncomp = calculate_overlap_ratio(non_comp_df, merged_intervals, overlap_intervals_col='overlap_intervals')
        # 合并所有 overlap_intervals 列中的区间
        noncomp_merged_intervals = merge_all_overlap_intervals(non_comp_df, overlap_intervals_col='overlap_intervals')
        
        # 计算部分
        comp_df = add_non_overlap_length_to_df(comp_df, ts_col='ts', dur_col='dur', merged_intervals=merged_intervals)
        # 合并所有 overlap_intervals 列中的区间
        comp_merged_intervals = merge_all_overlap_intervals(comp_df, overlap_intervals_col='overlap_intervals')

        # 非计算的时间需要和计算时间作差A1-A1∩A2 
        inter_comp_noncomp = intersect_intervals(noncomp_merged_intervals, comp_merged_intervals)
        total_inter_length = calculate_total_interval_length(inter_comp_noncomp)
        overlap_ratio_inter_comp_noncomp = total_inter_length / total_merged_intervals_time * 100
        
        # 其实算单独的时候也用到了A1∩A2 这里面效率是可以提升的
        only_noncomp = subtract_intervals(noncomp_merged_intervals, comp_merged_intervals)
        total_sub_length = calculate_total_interval_length(only_noncomp)
        overlap_ratio_only_noncomp = total_sub_length / total_merged_intervals_time * 100
            
        overlap_ratio_comp = calculate_overlap_ratio(comp_df, merged_intervals, overlap_intervals_col='overlap_intervals')
        print(f"非计算时长交叠{target_name}总时长的占比: {overlap_ratio_noncomp:.2f}%")
        print(f"计算时长交叠{target_name}总时长的占比: {overlap_ratio_comp:.2f}%")
        # 将结果写入 result_df
        result_df.loc[result_df['name_name'] == target_name, 'noncomp_ratio'] = overlap_ratio_noncomp
        result_df.loc[result_df['name_name'] == target_name, 'comp_ratio'] = overlap_ratio_comp
        result_df.loc[result_df['name_name'] == target_name, 'inter_comp_noncomp_ratio'] = overlap_ratio_inter_comp_noncomp
        result_df.loc[result_df['name_name'] == target_name, 'only_noncomp_ratio'] = overlap_ratio_only_noncomp
        result_df.loc[result_df['name_name'] == target_name, 'only_comp_ratio'] = overlap_ratio_comp - overlap_ratio_inter_comp_noncomp
        result_df.loc[result_df['name_name'] == target_name, 'free_ratio'] = 100 - overlap_ratio_comp - overlap_ratio_noncomp + overlap_ratio_inter_comp_noncomp

    # 增加可视化


    return result_df
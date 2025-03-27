# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from collections import defaultdict
from typing import Dict, List, TYPE_CHECKING

import pandas as pd
import plotly.express as px

from hta.utils.utils import get_kernel_type, KernelType, merge_kernel_intervals,shorten_name
from hta.utils.fjs_utils import add_non_overlap_length_to_df,calculate_overlap_ratio,merge_all_overlap_intervals,filter_and_process_data, \
                                intersect_intervals,subtract_intervals,calculate_total_interval_length,analyze_compute_stage

# import statement used without the "if TYPE_CHECKING" guard will cause a circular
# dependency with trace_analysis.py causing mypy to fail and should not be removed.
if TYPE_CHECKING:
    from hta.trace import Trace


class ForwardStageAnalysis:
    def __init__(self):
        
        pass

    @classmethod
    def get_forward_compute_stage_analysis(cls, t: "Trace", visualize: bool = True) -> pd.DataFrame:
        """
        Communication analysis implementation. See `get_comm_comp_overlap` in `trace_analysis.py` for details.
        """
        sym_table = t.symbol_table.get_sym_table()

        def get_forward_compute_stage_analysis_df(trace_df: pd.DataFrame) -> float:

            trace_df["name_name"] = trace_df["name"].apply(lambda x:sym_table[x])
            trace_df["cat_name"] = trace_df["cat"].apply(lambda x:sym_table[x])
            trace_df["kernel_type"] = trace_df["name_name"].apply(lambda x:get_kernel_type(x))
            trace_df["name_s"] = trace_df["name_name"].apply(lambda x:shorten_name(x))

            # 筛选用来分析的数据
            # 筛选 COMPUTATION 数据，且 stream 不为 -1，gpu kernal 层面 
            comp_df = trace_df[(trace_df['kernel_type'] == 'COMPUTATION') & (trace_df['stream'] != -1)]
            non_comp_df = trace_df[(trace_df['kernel_type'] != 'COMPUTATION') & (trace_df['stream'] != -1)]

            # 在原dataframe中筛选出人为打标 forward 的数据
            _, result_df = filter_and_process_data(
                trace_df, 
                cat_name_filter="user_annotation", 
                search_pattern="forward", # 传backward也行
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

            return result_df


        for rank, trace_df in t.traces.items():
            # 目前还只考虑支持rank 0
            result_df = get_forward_compute_stage_analysis_df(trace_df)

        if visualize:  # pragma: no cover
            # 定义饼图的分类标签
            labels = {
                "inter_comp_noncomp_ratio": "Inter Comp & NonComp",
                "only_comp_ratio": "Only Comp",
                "only_noncomp_ratio": "Only NonComp",
                "free_ratio": "Free",
            }

            # 遍历每一行，绘制交互式饼图
            for index, row in result_df.iterrows():
                name = row["name_name"]
                values = row[list(labels.keys())].tolist()
                
                fig = px.pie(
                    values=values,
                    names=list(labels.values()),
                    title=f"Ratio Distribution for <b>{name}</b>",
                    color_discrete_sequence=px.colors.qualitative.Pastel,  # 配色方案
                    hole=0.3,  # 空心饼图（可选）
                )
                
                fig.update_traces(
                    textposition="inside",
                    textinfo="percent+label",  # 显示百分比和标签
                    hoverinfo="label+percent",  # 悬停显示
                )
                
                fig.update_layout(
                    uniformtext_minsize=12,  # 字体大小
                    # uniformtext_mode="hide",  # 文字自动隐藏（如果太小）
                )
                
                fig.show()

        return result_df[["name_name", "total_dur", "noncomp_ratio", "comp_ratio", "inter_comp_noncomp_ratio","only_comp_ratio", "only_noncomp_ratio", "free_ratio"]]

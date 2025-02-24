import os
import sys
import json
import time

from hta.trace_analysis import TraceAnalysis

def df_to_dict_rc(df):
    custom_dict = {}
    for idx, row in df.iterrows():
        for col in df.columns:
            custom_dict['{}_{}'.format(idx, col)] = row[col]
    return custom_dict


def hta_analysis(trace_dir):
    analyzer = TraceAnalysis(trace_dir=trace_dir)
    res_dict = {}

    temporal_breakdown_df = analyzer.get_temporal_breakdown(visualize=False)
    res_dict.update(temporal_breakdown_df.mean(axis=0).to_dict())

    kernel_type_metrics_df, kernel_metrics_df = analyzer.get_gpu_kernel_breakdown(visualize=False)
    kernel_type_metrics_df = kernel_type_metrics_df.set_index("kernel_type")
    res_dict.update(df_to_dict_rc(kernel_type_metrics_df))

    idle_time_df, _ = analyzer.get_idle_time_breakdown(visualize=False)
    idle_time_df = idle_time_df.groupby('idle_category')[['idle_time']].sum()
    res_dict.update(df_to_dict_rc(idle_time_df))

    overlap_df = analyzer.get_comm_comp_overlap(visualize=False)
    res_dict.update(overlap_df.mean(axis=0).to_dict())

    return res_dict

def get_trace_dir(log_dir):
    trace_dirs = []
    for root, dirs, files in os.walk(log_dir):
        for dir_name in dirs:
            if dir_name != "tensorboard":
                continue
            subdir_path = os.path.join(root, dir_name)
            trace_dirs.append(subdir_path)
    return trace_dirs

if __name__ == "__main__":
    log_dir = sys.argv[1]
    with open(os.path.join(log_dir, "hta_analysis_res"), "a") as fo:
        for trace_dir in get_trace_dir(log_dir):
            start_time = time.time()
            res_dict = hta_analysis(trace_dir)
            res_dict["used_time_sec"] = time.time()-start_time
            res_dict["trace_dir"] = trace_dir
            fo.write("{}\n".format(json.dumps(res_dict)))

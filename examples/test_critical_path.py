import os
os.environ['HTA_DISABLE_NS_ROUNDING'] = '1'
from hta.trace_analysis import TraceAnalysis
base_dir = "/mnt/self-define/zhangweixing/profiler_log/deepseek_v2/tensorboard/"
# trace_dir = base_dir + "20250325-1043_pretrain-zjmcore-dsv3-16B-lr-4E-5-minlr-4E-6-bs-1-gbs-32-seqlen-4096-pr-bf16-pp-2-ac-none/"
trace_dir = base_dir + "z2000-16b/"

analyzer = TraceAnalysis(trace_dir=trace_dir)

instance_id = 0  # note this is zero based
annotation = "ProfilerStep" #"forward_model_compute" #"ProfilerStep"  # will match multiple ProfilerStepXXX annotations
cp_graph, success = analyzer.critical_path_analysis(rank = 0, annotation=annotation, instance_id=instance_id)
cp_graph._show_digraph()
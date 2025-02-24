import sys
import json
import pandas as pd

def report_v1(log_path, report_path):
    data_list = []
    with open(log_path) as f:
        for line in f:
            data_dict = json.loads(line)
            data_list.append(data_dict)
    print(json.dumps(data_list))
    df = pd.DataFrame(data_list)
    df.to_csv(report_path)


if __name__ == "__main__":
    log_path = sys.argv[1]
    output_path = sys.argv[2]
    report_v1(log_path, output_path)
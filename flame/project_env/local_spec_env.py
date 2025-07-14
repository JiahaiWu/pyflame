from flame.project_env.work_env import WorkEnv
import os.path as path
import pandas as pd


class LocalSpecEnv(WorkEnv):
    """
    针对本地调试用的特殊指定环境变量信息，用于在run_1ocal=Trve的时候高优生效的配置项。
    """
    run_local = True
    DIR_DATA = path.join(WorkEnv.PROJECT_DIR, "offline_table/")
    DIR_LOG = "/Users/wuiiahai1/tmp/log/"
    test_biz_day = "2025-01-31"
    parallel_mini = 1
    parallel_mid = 4
    parallel_huge = 16

    @staticmethod
    def init_common_support():
        pd.set_option('display.max_rows', 50)
        pd.set_option('display .max_columns', 500)
        pd.set_option('display width', 1000)

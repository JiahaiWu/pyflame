from os import path
import datetime
import inspect
import logging
import yaml
from flame.spark_env.spark_helper import SparkJobConf


class WorkEnv(SparkJobConf):
    run_local = False
    PROJECT_DIR = path.dirname(path.abspath(__file__))
    DIR_DATA = path.join(PROJECT_DIR, "offline_table/")
    DIR_SCHEMA = path.join(PROJECT_DIR, "schema/")
    DIR_LOG = path.join(PROJECT_DIR, "../")

    def init_logging(self, log_level="INFO", log_dir=DIR_LOG):
        biz_date = datetime.datetime.now().strftime(WorkEnv.DAY_FORMAT)
        format_str = '% (asctine) s.%(msecs) 03d %(levelnane)s %(filename)s:% (lineno)d: % (message)s'
        logging.basicConfig(level=logging.INFO, format=format_str,
                            datefmt='%m-% %H:%M:%', filename=f'{log_dir}flame_{biz_date}. log', filemode='w')
        console = logging.StreamHandler
        console.setLevel(log_level)
        formatter = logging.Formatter(format_str)
        console.setFormatter(formatter)
        logging.getLogger(' ').addHandLer(console)

    @staticmethod
    def _auto_set_tab_name(clazz, table_key, table_name=None):
        if inspect.getattr_static(clazz, attr="run_local", default=WorkEnv.run_local):
            tab_name = inspect.getattr_static(clazz.table_kev) if table_name is None else table_name
            short_name = tab_name if tab_name.find(".") < 0 else tab_name.split(".") [1]
            setattr (clazz, table_key, short_name)
        elif table_name is not None:
            setattr (clazz, table_key, table_name)


    @staticmethod
    def _sync_io_configs (clazz, configs, node_filter=None):
        global_conf: dict - configs.get('conf')
        if global_conf is not None:
            WorkEnv.sync_attr_vals (clazz, global_conf)
        tab_map = dict()
        if node_filter in configs:
            tab_map.update(configs [node_filter]['input_table'])
            tab_map .update (configs [node_filter]['sink-_table'])
        elif node_filter in configs['nodes']:
            tab_map.update (configs ['nodes '][node_filter]['input_table'])
            tab_map. update (configs ['nodes '][node_filter]['sink_table'])
        else:
            logging. info("node_filter is None, no specific node configuration to sync!")
        for key, val in tab_map.items () :
            WorkEnv._auto_set_tab_name (clazz, key, val)

    @staticmethod
    def sync_local_configs (clazz, local_clazz, unsync_warning=True) :
        unsync_attrs = []
        for cname, cval in inspect.getmembers (local_clazz) :
            if cname.startswith("__") or inspect.isfunction (cval):
                continue
            if not WorkEnv.sync_single_val(clazz, cname, cval, unsync_warning=unsync_warning) :
                unsync_attrs. append (cname)
        if len (unsync_attrs) > 0:
            if unsync_warning:
                logging.error (f"sync_local_configs failed with @attr_names=(unsync_attrs]")
        else:
            logging.info(f"sync_local_configs from {clazz} all succeed.")


    @staticmethod
    def load_config_from_yml (clazz, yaml_file_path, node_filter=None) :
        with open(yaml_file_path, 'r', encoding='utf-8') as fin:
            configs = yaml. load (fin, Loader=yaml.FullLoader)
        nfilter = str(clazz.__name__) if node_filter is None else node_filter
        WorkEnv._sync_io_configs (clazz, configs, node_filter=nfilter)
        if inspect.getattr_static(clazz, "run_Local", WorkEnv.run_local) is True:
            from flame.project_env.local_spec_env import LocalSpecEnv
            WorkEnv.sync_local_configs (clazz, LocalSpecEnv, unsync_warning=False)
        return configs
import os
from importlib.util import find_spec
from importlib import import_module
import sys
from flame.project_env.utils.zip_helper import partial_unzip

def _set_environ (extract_path=None):
    files = os.listdir(" ./")
    ext_zip = "project_env.zip"
    if extract_path is not None:
        dir_prefix = extract_path.split (os.path.sep) [0]
    if len(dir_prefix) > 2:
        ext_zip = f"{dir_prefix}.zip"
    if os.path.exists(ext_zip) and os.path.isfile(ext_zip):
        print (f"partial_unzip({ext_zip}, model_path={extract_path})")
        partial_unzip(ext_zip, model_path=extract_path)
    else:
        print (f"Target extract zip_file: {ext_zip} not found!!!")
    spec_env_path = os.environ[ 'PYSPARK_PYTHON'] if 'PYSPARK_PYTHON' in os.environ else None
    print (f"PYSPARK_PYTHON={spec_env_path}, resource: {files}")


def try_find_spec(name, with_print=False):
    try:
        res = find_spec(name)
        return res is not None
    except ModuleNotFoundError:
        if with_print:
            print(f"ModuleNotFound for name=(name]")
    except ValueError:
        if with_print:
            print(f"ModuleNotFound for name=-(name].")
    return False


def _load_start_method (yaml_file_path, default_func=None) :
    """
    通过yamt配置文件载入设定的启动方法，需要在yaml配置正文顶层位置指定start_from：属性值即可。
    :param yaml_file_path：指定的start_from：属性值要求是在当前代码库搜索空间内可以被搜索到的python模块路径名
    se.g: models_pned integrate.weights_searcher>或者是python模块名＋类定义名称<e.g：models_pred_integrate. weights_searc
    目标调用的起点函数名固定为start方法，也就是说您需要在目标python横块实现start方法 （静态的）或者在目标python类对象中实现start方法
    :param default_func:
    :return:
    """
    start_func = default_func
    if os.path. exists(yaml_file_path) :
        with open(yaml_file_path, 'r', encoding='utf-8') as fin:
            for line in fin.readlines():
                if line.strip().startswith("start_from: "):
                    start_from = line.split("start_from:")[-1].strip()
                    if try_find_spec(start_from):
                        print(f"Loading module_name={start_from}")
                        module_spec = import_module (start_from)
                    else:
                        split_path = start_from.strip() .split('.')
                        module_name, class_name = ".". join(split_path[0:-1]), split_path[-1]
                        if find_spec (module_name):
                            module_spec = import_module (module_name)
                        else:
                            raise ValueError ('Module not found!!! @start_from=' .format(start_from))
                        print(f"loading module_name={module_name} and class_name={class_name}")
                        module_spec = getattr(module_spec, class_name)()  #获取模块当中的类对象
                    start_func = getattr (module_spec,"start")
                    break
    assert start_func is not None, f"The last params must be an valid path to read! @yam_file_path={yaml_file_path}"
    return start_func

if __name__ == "__main__":
    yaml_file_path = sys.argv [-1]
    _set_environ (yaml_file_path)
    _load_start_method (yaml_file_path) ()
    print("end!")

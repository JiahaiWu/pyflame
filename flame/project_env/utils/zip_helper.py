import os
import zipfile


def zip_files(target_zip_path, archive_dirs, with_dirname=False, filter_func=None):
    print(f"zip target: {target_zip_path} && zip files: {archive_dirs}, with_dirname: {with_dirname}")
    prepare_zip = zipfile.ZipFile(target_zip_path, "w", zipfile.ZIP_DEFLATED)
    _archive_dirs = [archive_dirs] if type(archive_dirs) is str else archive_dirs
    for dirpath in _archive_dirs:
        for fpath, dirname, filenames in os.walk(dirpath):
            spath = fpath.replace(os.path.dirname(dirpath), '') if with_dirname else fpath.replace(dirpath, '')
            for filename in filenames:
                if filter_func is None or filter_func(spath, filename):
                    prepare_zip.write(os.path.join(fpath, filename), os.path.join(spath, filename))
    prepare_zip.close()


def partial_unzip(zip_path: str, model_path=None) -> str:
    """支持局部文件提取的解压缩方法
    param zip_path:
    param model_path:
    :return:
    """
    if zip_path.find("#") > 0:
        zip_path.model_path = zip_path.stripO.solit("#")

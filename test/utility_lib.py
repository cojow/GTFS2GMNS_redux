# -*- coding:utf-8 -*-
##############################################################
# Created Date: Wednesday, November 16th 2022
# Contact Info: luoxiangyong01@gmail.com
# Author/Copyright: Mr. Xiangyong Luo
##############################################################

import os
import datetime
from pathlib import Path
from typing import Union  # Python version <= 3.9


# A decorator to measure the time of a function
def func_running_time(func):
    def inner(*args, **kwargs):
        print(f'INFO Begin to run function: {func.__name__} …')
        time_start = datetime.datetime.now()
        res = func(*args, **kwargs)
        time_diff = datetime.datetime.now() - time_start
        print(
            f'INFO Finished running function: {func.__name__}, total: {time_diff.seconds}s')
        print()
        return res
    return inner


# convert OS path to standard linux path
def path2linux(path: Union[str, Path]) -> str:
    """Convert a path to a linux path, linux path can run in windows, linux and mac"""
    try:
        return path.replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def get_txt_files_from_folder(dir_name: str, file_type: str = "txt", isTraverseSubdirectory: bool = False) -> list:
    if isTraverseSubdirectory:
        files_list = []
        for root, dirs, files in os.walk(dir_name):
            files_list.extend([os.path.join(root, file) for file in files])
        return [path2linux(file) for file in files_list if file.split(".")[-1] == file_type]

    # files in the first layer of the folder
    return [path2linux(os.path.join(dir_name, file)) for file in os.listdir(dir_name) if file.split(".")[-1] == file_type]


def check_required_files_exist(required_files: list, dir_files: list) -> bool:
    # mask have the same length as required_files
    mask = [file in dir_files for file in required_files]
    if all(mask):
        return True

    print(f"Error: Required files are not satisfied, missing files are: {[required_files[i] for i in range(len(required_files)) if not mask[i]]}")

    return False


if __name__ == "__main__":
    dir_name = r"C:\Users\roche\Anaconda_workspace\001_Github.com\GTFS2GMNS\test\GTFS"

    files_from_folder_abspath = get_txt_files_from_folder(dir_name, isTraverseSubdirectory=False)

    files_required = ["agency.txt", "routes.txt", "shapes.txt", "stops.txt", "trips.txt"]
    required_files_abspath = [path2linux(os.path.join(dir_name, file)) for file in files_required]

    isFilesExist = check_required_files_exist(required_files_abspath, files_from_folder_abspath)

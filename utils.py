
from unicodedata import normalize
import os
import time


def delete_folder(folder):

    cmd = os.system('rm -rf {0}'.format(folder))
    if cmd != 0:
        raise Exception('Could not delete folder')


def get_last_folder(bkp_folder_path):

    path_list = bkp_folder_path.split('/')
    path_clean_list = [x for x in path_list if x != '']
    folder_name = path_clean_list[len(path_clean_list) - 1]
    return folder_name


def get_last_folder_path(bkp_folder_path):
    path_list = bkp_folder_path.split('/')
    path_clean_list = [x for x in path_list if x != '']
    folder_name = path_clean_list[len(path_clean_list) - 1]
    path = bkp_folder_path.replace(folder_name, '')
    return path


def zip_folder(bkp_folder_path):
    path = get_last_folder_path(bkp_folder_path)
    try:
        os.chdir(path)
    except OSError:
        raise Exception('Could not reach in backup folder path to zip folder')

    folder_name = get_last_folder(bkp_folder_path)
    cmd = os.system('zip -r {0}.zip {0}'.format(folder_name))
    if cmd != 0:
        raise Exception('Could not zip folder')


def delete_old_files(days_delete, folder):
    folders_deleted = 0
    time_now = (time.time() - (days_delete * 86400))

    files = os.listdir(folder)
    for xfile in files:
        file_dir = os.path.join(folder + xfile)
        date_file = os.stat(file_dir).st_ctime
        if date_file <= time_now:
            os.remove(file_dir)
            folders_deleted = folders_deleted + 1

    return folders_deleted


def clear_name(name):
    reg1 = ')'
    reg2 = '('
    name = name.replace(' ', '').strip()
    if name != '' and reg1 not in name and reg2 not in name:
        return name


def remover_acentos(txt, codif='utf-8'):
    return normalize('NFKD', txt.decode(codif)).encode('ASCII', 'ignore')
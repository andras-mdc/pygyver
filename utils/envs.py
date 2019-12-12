import os


def save_requirements(file_path='requirements.txt'):
    cmd = 'pip freeze > {}/{}'.format(os.getcwd(), file_path)
    os.system(cmd)


def install_requirements(file_path='requirements.txt'):
    cmd = 'pip install -r {}/{}'.format(os.getcwd(), file_path)
    os.system(cmd)


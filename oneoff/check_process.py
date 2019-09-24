import psutil
import os


def is_running(process_id, _file, args=None):
    if not args:
        arguments = {_file: False}
    else:
        arguments = {_file: False}
        if type(args) != list:
            args = args.split(',')
        for arg in args:
            arguments[arg] = False

    for pid in psutil.pids():
        p = psutil.Process(pid)
        cmdline = p.cmdline()
        for argument in arguments:
            if argument in cmdline:
                arguments[argument] = True
            else:
                arguments[argument] = False

        if all(x for x in arguments.values()):
            if int(pid) != int(process_id):
                return True

    return False


fullpath = os.path.realpath(__file__)
path, _file = os.path.split(fullpath)
if not is_running(os.getpid(), _file, []):
    # do somehing
    pass

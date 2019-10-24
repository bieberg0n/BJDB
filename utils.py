import pprint


def log(*args):
    if len(args) == 1:
        pprint.pprint(*args)
    else:
        print(*args)

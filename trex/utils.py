import warnings


def list_or_args(command, keys, args):
    oldapi = bool(args)
    try:
        iter(keys)
        if isinstance(keys, (str, unicode)):
            keys = [keys]
            if not oldapi:
                return keys
            oldapi = True
    except TypeError:
        oldapi = True
        keys = [keys]

    if oldapi:
        warnings.warn(DeprecationWarning(
            "Passing *args to redis.%s is deprecated. "
            "Pass an iterable to ``keys`` instead" % command))
        keys.extend(args)
    return keys

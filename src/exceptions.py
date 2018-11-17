import rpyc

def vinegarify(remote_name):
    def deco(cls):
        rpyc.core.vinegar._generic_exceptions_cache[remote_name] = cls
        return cls
    return deco

class DfsException(BaseException):
    pass


class DfsHttpException(DfsException):
    pass

@vinegarify
class DfsNoFreeStorages(BaseException):
    pass
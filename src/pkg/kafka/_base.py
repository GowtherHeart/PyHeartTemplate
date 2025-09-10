import hashlib


class _Singleton(type):
    """
    A metaclass for creating singleton classes with parameter-based instantiation.

    Unlike the traditional singleton pattern, this implementation allows for multiple
    instances of a class, each associated with a unique set of initialization parameters.
    The uniqueness of each instance is determined by hashing the provided arguments and
    keyword arguments. If an instance with the same parameter hash already exists, it is
    returned; otherwise, a new instance is created and stored.

    Attributes:
        _inst_map (dict): A dictionary mapping parameter hashes to their corresponding instances.
    """

    _inst_map: dict = {}

    def _merge_param(cls, *args, **kwargs) -> str:
        result = []
        for v in args:
            result.append(str(v))
        for _, v in kwargs.items():
            result.append(str(v))

        m = hashlib.sha256()
        m.update("".join(result).encode("utf-8"))
        return m.hexdigest()

    def __call__(cls, *args, **kwargs):
        param_hex = cls._merge_param(*args, **kwargs)
        if param_hex not in cls._inst_map:
            cls._inst_map[param_hex] = super(_Singleton, cls).__call__(*args, **kwargs)

        return cls._inst_map[param_hex]

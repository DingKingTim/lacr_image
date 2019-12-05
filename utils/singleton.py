
class Singleton(type):
    def __new__(mcs, name, bases, attrs):
        attrs['_instance'] = None
        return super(Singleton, mcs).__new__(mcs, name, bases, attrs)

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instance


class SbFeedError(Exception):
    pass


class AlreadyExistsError(SbFeedError):
    pass


class NotExistError(SbFeedError):
    pass


class CommandNotSupportedError(SbFeedError):
    pass

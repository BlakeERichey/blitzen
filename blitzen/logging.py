import logging

__all__ = [
  'get_logger', 'set_logfile', 'set_loglevel', 
  'disable', 'enable'
]

LOGGER_NAME = 'blitzen'
LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
_logger = None
_log_level = logging.INFO

def get_logger():
  global _logger
  global _log_level
  if not _logger:
    _logger = logging.getLogger(LOGGER_NAME)
    formatter = logging.Formatter(
      '%(asctime)s %(name)-8s %(levelname)-8s %(message)s',
      datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    _logger.addHandler(handler)
    _logger.setLevel(_log_level)

  return _logger

def set_logfile(filename):
  global _logger
  global _log_level
  if _logger:
    fh = logging.FileHandler(filename)
    fh.setLevel(_log_level)
    _logger.addHandler(fh)

def set_loglevel(level):
  global _logger
  global _log_level
  if not _logger:
    _log_level = level
    _logger.setLevel(_log_level)

def disable():
  global _logger
  if not _logger:
    _logger = get_logger()
  _logger.disabled = True

def enable():
  global _logger
  if not _logger:
    _logger = get_logger()
  _logger.disabled = False
import logging


# Custom formatter
class LogFormatter(logging.Formatter):

    err_fmt = '%(asctime)s %(name)s %(levelname)s %(module)s : line  %(lineno)d : %(message)s'
    dflt_fmt = '%(asctime)s %(name)s %(levelname)s %(message)s'

    def __init__(self):
        super().__init__(fmt=LogFormatter.dflt_fmt, datefmt=None, style='%')

    def format(self, record):

        # Save the original format configured by the user
        # when the logger formatter was instantiated
        format_orig = self._style._fmt

        # Replace the original format with one customized by logging level
        if record.levelno == logging.ERROR:
            self._style._fmt = LogFormatter.err_fmt

        # Call the original formatter class to do the grunt work
        result = logging.Formatter.format(self, record)

        # Restore the original format configured by the user
        self._style._fmt = format_orig

        return result


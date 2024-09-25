from pandas import Timedelta


class measurement:
    def __init__(self, init_data=None, instrument="LI-7810", instrument_id=None):
        if init_data is not None and init_data:
            self.init_measurement(init_data)
            self.instrument = instrument
        else:
            self.id = None
            self.start = None
            self.close = None
            self.open = None
            self.end = None
            self.instrument = None
            self.instrument_id = None

    def init_measurement(self, filter_tuple):
        """
        Initiate class from namedtuple.

        Parameters
        ----------
        filter_tuple : namedtuple


        """
        self.id = filter_tuple.id
        self.start = filter_tuple.start
        self.close = filter_tuple.close
        self.open = filter_tuple.open
        self.end = filter_tuple.end
        self.plot_end = filter_tuple.end + Timedelta(minutes=2)
        self.plot_start = filter_tuple.start - Timedelta(minutes=2)
        self.doy = filter_tuple.start.dayofyear
        self.date = filter_tuple.start.date()
        self.month = filter_tuple.start.month
        self.day = filter_tuple.start.day
        self.week = filter_tuple.start.week

    def calc_flux(self):
        pass

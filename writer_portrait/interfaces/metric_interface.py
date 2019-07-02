from abc import ABCMeta, abstractmethod


class DSMetricsInterface:
    __metaclass__ = ABCMeta

    ID = None

    @abstractmethod
    def get_id(self) -> int:
        """ Get Metric ID """

    @abstractmethod
    def get_name(self) -> str:
        """ Get Metric Name """

    @abstractmethod
    def get_median(self) -> float:
        """ Get Metric Median """

    @abstractmethod
    def get_q20(self) -> float:
        """ Get 20 Quantile """

    @abstractmethod
    def get_q80(self) -> float:
        """ Get 80 Quantile """

    @abstractmethod
    def get_ban_threshold(self) -> float:
        """ Get Ban threshold """

    @abstractmethod
    def get_mean_std(self) -> float:
        """ Get Mean STD """

    @abstractmethod
    def get_std_mean(self) -> float:
        """ Get STD Mean """

    @abstractmethod
    def get_signal2noise_ratio(self) -> float:
        """ Get Signal To Noise Ratio"""

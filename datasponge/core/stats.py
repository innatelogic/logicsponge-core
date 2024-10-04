from typing import Any

import numpy as np
import scipy.stats

import datasponge.core as ds


class BaseStatistic(ds.FunctionTerm):
    """Base class to handle common functionality for base statistics."""

    dim: int
    stat_name: str

    def __init__(self, *args, dim: int = 1, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.dim = dim
        self.stat_name = "base_statistic"

    def calculate(self, *args, **kwargs):
        raise NotImplementedError

    def run(self, ds_view: ds.DataStreamView):
        ds_view.next()
        self._latency_queue.tic()

        if self.dim == 0:
            keys = ds_view[-1].keys()
            results = ds.DataItem(
                {key: self.calculate(np.array(ds_view.key_to_list(key), dtype=float)) for key in keys}
            )
            self.output(results)

        elif self.dim == 1:
            values = np.array(list(ds_view[-1].values()), dtype=float)
            result = self.calculate(values)
            if isinstance(result, dict):
                self.output(ds.DataItem(result))
            else:
                out = ds.DataItem({self.stat_name: result})
                self.output(out)

        else:
            msg = f"Unknown dimension {self.dim}"
            raise ValueError(msg)

        self._latency_queue.toc()


class Sum(ds.FunctionTerm):
    """Computes cumulative sum of `key` over data items."""

    def __init__(self, *args, key: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        self.state = 0.0  # initially, sum is 0

    def f(self, item: ds.DataItem) -> ds.DataItem:
        self.state += item[self.key]
        return ds.DataItem({"sum": self.state})


class Mean(BaseStatistic):
    """Computes mean per data item."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stat_name = "mean"

    def calculate(self, values: np.ndarray) -> float:
        mean = np.mean(values)
        return float(mean)


class Std(BaseStatistic):
    """Computes standard deviation per data item."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stat_name = "std"

    def calculate(self, values: np.ndarray) -> float:
        std = np.std(values)
        return float(std)


class StdHull(BaseStatistic):
    """Computes mean, lower bound, and upper bound per data item with configurable standard deviation factor.

    dim:
        0: statistics over time per key
        1: statistics over keys per time-point
    """

    factor: float

    def __init__(self, *args, factor: float = 1.0, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.factor = factor

    def calculate(self, values: np.ndarray) -> dict[str, Any]:
        mean = np.mean(values)
        std = np.std(values)
        lower_bound = mean - self.factor * std
        upper_bound = mean + self.factor * std
        return {"mean": mean, "lower_bound": lower_bound, "upper_bound": upper_bound}


class TestStatistic(ds.FunctionTerm):
    """Base class to handle common functionality for test statistics."""

    arity: int | None
    dim: int
    stat_name: str

    def __init__(self, *args, dim: int = 1, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.arity = None
        self.dim = dim

    def calculate(self, *args, **kwargs):
        raise NotImplementedError

    def run(self, ds_view: ds.DataStreamView):
        ds_view.next()
        self._latency_queue.tic()

        if self.dim == 0:
            keys = ds_view[-1].keys()
            if self.arity is not None and len(keys) != self.arity:
                msg = f"Arity of test statistic does not match number of inputs {len(keys)}"
                raise ValueError(msg)
            series_list = [np.array(ds_view.key_to_list(key), dtype=float) for key in keys]
            result = self.calculate(*series_list)
            self.output(ds.DataItem(result))

        elif self.dim == 1:
            if self.arity != 1:
                msg = f"Arity of test statistic is {self.arity} but should be 1"
                raise ValueError(msg)
            values = np.array(list(ds_view[-1].values()), dtype=float)
            result = self.calculate(values)
            self.output(ds.DataItem(result))

        else:
            msg = f"Unknown dimension {self.dim}"
            raise ValueError(msg)

        self._latency_queue.toc()


class OneSampleTTest(TestStatistic):
    """Performs t-Test."""

    def __init__(self, *args, mean: float = 0.0, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.arity = 1
        self.mean = mean

    def calculate(self, values: np.ndarray) -> dict[str, Any]:
        if len(values) <= 1:
            return {"t-statistic": None, "p-value": None}
        t_statistic, p_value = scipy.stats.ttest_1samp(values, self.mean)
        return {"t-statistic": t_statistic, "p-value": p_value}


class PairedTTest(TestStatistic):
    """Performs paired t-Test."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.arity = 2
        self.dim = 0

    def calculate(self, series1: np.ndarray, series2: np.ndarray) -> dict[str, Any]:
        if len(series1) <= 1 or len(series2) <= 1:
            return {"t-statistic": None, "p-value": None}
        t_statistic, p_value = scipy.stats.ttest_rel(series1, series2)
        return {"t-statistic": t_statistic, "p-value": p_value}


class KruskalWallis(TestStatistic):
    """Performs Kruskal-Wallis-Test."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.arity = None
        self.dim = 0

    def calculate(self, *series: np.ndarray) -> dict[str, Any]:
        if any(len(s) <= 1 for s in series):
            return {"h-statistic": None, "p-value": None}

        h_statistic, p_value = scipy.stats.kruskal(*series)
        return {"h-statistic": h_statistic, "p-value": p_value}

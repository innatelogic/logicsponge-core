"""Statistics for logcisponge."""

from typing import Any

import numpy as np
import scipy.stats

import logicsponge.core as ls


class BaseStatistic(ls.FunctionTerm):
    """Base class to handle common functionality for base statistics."""

    dim: int
    stat_name: str

    state: ls.State

    def __init__(self, *args, dim: int = 1, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create an instance."""
        super().__init__(*args, **kwargs)

        self.dim = dim
        self.stat_name = "base_statistic"

        self.state = {}

    def calculate(self, *args, **kwargs) -> Any:  # noqa: ANN002, ANN003, ANN401
        """Calculate the statistics."""
        raise NotImplementedError

    def f(self, di: ls.DataItem) -> ls.DataItem:
        """Collect data and calculate the statistics."""
        if self.dim == 0:
            for key in di:
                if key not in self.state:
                    self.state[key] = []
                self.state[key].append(di[key])

            results = ls.DataItem({key: self.calculate(np.array(self.state[key], dtype=float)) for key in di})
            self.output(results)

        if self.dim == 1:
            values = np.array(list(di.values()), dtype=float)
            result = self.calculate(values)

            if isinstance(result, dict):
                return ls.DataItem(result)

            return ls.DataItem({self.stat_name: result})

        msg = f"Unknown dimension {self.dim}"
        raise ValueError(msg)


class Sum(ls.FunctionTerm):
    """Computes cumulative sum of `key` over data items."""

    def __init__(self, *args, key: str, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create an instance."""
        super().__init__(*args, **kwargs)
        self.key = key
        self.state["value"] = 0.0  # initially, sum is 0

    def f(self, item: ls.DataItem) -> ls.DataItem:
        """Run on new data."""
        self.state["value"] += item[self.key]
        return ls.DataItem({"sum": self.state["value"]})


class Mean(BaseStatistic):
    """Computes mean per data item."""

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create an instance."""
        super().__init__(*args, **kwargs)
        self.stat_name = "mean"

    def calculate(self, values: np.ndarray) -> float:
        """Perform the calculation of the statistics."""
        mean = np.mean(values)
        return float(mean)


class Std(BaseStatistic):
    """Computes standard deviation per data item."""

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create an instance."""
        super().__init__(*args, **kwargs)
        self.stat_name = "std"

    def calculate(self, values: np.ndarray) -> float:
        """Perform the calculation of the statistics."""
        std = np.std(values)
        return float(std)


class StdHull(BaseStatistic):
    """Computes mean, lower bound, and upper bound per data item with configurable standard deviation factor.

    dim:
        0: statistics over time per key
        1: statistics over keys per time-point
    """

    factor: float

    def __init__(self, *args, factor: float = 1.0, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create an instance."""
        super().__init__(*args, **kwargs)
        self.factor = factor

    def calculate(self, values: np.ndarray) -> dict[str, Any]:
        """Perform the calculation of the statistics."""
        mean = np.mean(values)
        std = np.std(values)
        lower_bound = mean - self.factor * std
        upper_bound = mean + self.factor * std
        return {"mean": mean, "lower_bound": lower_bound, "upper_bound": upper_bound}


class TestStatistic(ls.FunctionTerm):
    """Base class to handle common functionality for test statistics."""

    arity: int | None
    dim: int
    stat_name: str

    state: ls.State

    def __init__(self, *args, dim: int = 1, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create an instance."""
        super().__init__(*args, **kwargs)

        self.arity = None
        self.dim = dim

        self.state = {}

    def calculate(self, *args, **kwargs) -> Any:  # noqa: ANN002, ANN003, ANN401
        """Perform the calculation of the statistics."""
        raise NotImplementedError

    def f(self, di: ls.DataItem) -> ls.DataItem:
        """Collect data and calculate the statistics."""
        if self.dim == 0:
            if self.arity is not None and len(di) != self.arity:
                msg = f"Arity of test statistic does not match number of inputs {len(di)}"
                raise ValueError(msg)

            for key in di:
                if key not in self.state:
                    self.state[key] = []
                self.state[key].append(di[key])

            series_list = [np.array(self.state[key], dtype=float) for key in di]

            result = self.calculate(*series_list)
            return ls.DataItem(result)

        if self.dim == 1:
            if self.arity != 1:
                msg = f"Arity of test statistic is {self.arity} but should be 1"
                raise ValueError(msg)
            values = np.array(list(di.values()), dtype=float)
            result = self.calculate(values)
            return ls.DataItem(result)

        msg = f"Unknown dimension {self.dim}"
        raise ValueError(msg)


class OneSampleTTest(TestStatistic):
    """Performs a t-Test."""

    def __init__(self, *args, mean: float = 0.0, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create an instance."""
        super().__init__(*args, **kwargs)
        self.arity = 1
        self.mean = mean

    def calculate(self, values: np.ndarray) -> dict[str, Any]:
        """Perform the calculation of the statistics."""
        if len(values) <= 1:
            return {"t-statistic": None, "p-value": None}
        t_statistic, p_value = scipy.stats.ttest_1samp(values, self.mean)
        return {"t-statistic": t_statistic, "p-value": p_value}


class PairedTTest(TestStatistic):
    """Performs paired t-Test."""

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create an instance."""
        super().__init__(*args, **kwargs)
        self.arity = 2
        self.dim = 0

    def calculate(self, series1: np.ndarray, series2: np.ndarray) -> dict[str, Any]:
        """Perform the calculation of the statistics."""
        if len(series1) <= 1 or len(series2) <= 1:
            return {"t-statistic": None, "p-value": None}
        t_statistic, p_value = scipy.stats.ttest_rel(series1, series2)
        return {"t-statistic": t_statistic, "p-value": p_value}


class KruskalWallis(TestStatistic):
    """Performs Kruskal-Wallis-Test."""

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create an instance."""
        super().__init__(*args, **kwargs)
        self.arity = None
        self.dim = 0

    def calculate(self, *series: np.ndarray) -> dict[str, Any]:
        """Perform the calculation of the statistics."""
        if any(len(s) <= 1 for s in series):
            return {"h-statistic": None, "p-value": None}

        h_statistic, p_value = scipy.stats.kruskal(*series)
        return {"h-statistic": h_statistic, "p-value": p_value}

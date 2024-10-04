import logging
import math
from collections.abc import Callable
from typing import Self, TypedDict

# import matplotlib as mpl
import matplotlib.axes
import matplotlib.figure
import matplotlib.lines
import matplotlib.pyplot as plt
import numpy as np

import datasponge.core as ds

logger = logging.getLogger(__name__)


class PlotState(TypedDict):
    x: list[float]
    y: dict[str, list[float]]


class PlotParams(TypedDict):
    x: list[float]
    y: list[float]
    args: list
    kwargs: dict


class Plot(ds.FunctionTerm):
    """plot data items

    typical uses are:
    - Plot(x='a', y=['b', 'c'])
    - Plot(x='a', y='b')
    - Plot(y='b') : plot over round number
    - Plot() : plot all keys over round number
    """

    x_name: str
    y_names: list[str] | None
    state: PlotState
    lines: dict[str, matplotlib.lines.Line2D]
    fig: matplotlib.figure.Figure | None
    ax: matplotlib.axes.Axes | None
    incremental: bool

    def __init__(
        self, *argv, x: str = "round", y: str | list[str] | None = None, incremental: bool = True, **argk
    ) -> None:
        super().__init__(*argv, **argk)
        self.state = {
            "x": [],
            "y": {},
        }
        self.x_name = x
        if isinstance(y, str):
            self.y_names = [y]
        else:
            self.y_names = y
        self.lines = {}
        self.incremental = incremental
        self.fig, self.ax = plt.subplots()

    def _axis_setup(self, item: ds.DataItem) -> None:
        # check if need to discover the y_names
        if self.y_names is None:
            # we will plot all keys of the input
            # except for the x-key
            self.y_names = list(set(item.keys()) - {self.x_name})

        # init the y-value dict
        self.state["y"] = {k: [] for k in self.y_names}

        # init lines
        if self.ax is None:
            msg = "this should not happen: self.ax must be set on the 1st plot"
            raise ValueError(msg)

        self.lines = {y_name: self.ax.plot([], [], lw=2)[0] for y_name in self.y_names}
        self.ax.set_xlabel(self.x_name)
        self.ax.legend(self.y_names)
        self.ax.set_title(self.name)

    def plot(self, item: ds.DataItem) -> None:
        to_plot = item["plot"]
        self._axis_setup(to_plot)

        # update x-state
        if self.x_name == "round":
            round_nr = len(self.state["x"])
            self.state["x"] += [round_nr]
        else:
            self.state["x"] += [to_plot[self.x_name]]
            # this may fail if x is not a key

        # update y-state
        if self.y_names is None:
            msg = "this should not happen: self.y_names must be set"
            raise ValueError(msg)
        for k in self.y_names:
            self.state["y"][k] = to_plot[k]

        # update figure data
        m = float("inf")
        M = -float("inf")  # noqa: N806
        for k in self.y_names:
            self.lines[k].set_data(self.state["x"], self.state["y"][k])
            finite_values = [v for v in self.state["y"][k] if is_finite(v)]
            m = min([*finite_values, m])
            M = max([*finite_values, M])  # noqa: N806

        # rezoom
        if self.ax is None:
            msg = "this should not happen: self.ax must be set"
            raise ValueError(msg)
        self.ax.set_xlim(self.state["x"][0] - 1, self.state["x"][-1] + 1)
        y_offset = min(M - m, 1.0) * 0.1
        y_lower = m - y_offset if is_finite(m - y_offset) else -1.0
        y_upper = M + y_offset if is_finite(M + y_offset) else 1.0
        self.ax.set_ylim(y_lower, y_upper)

        # draw
        if self.fig is None:
            msg = "this should not happen: self.fig must be set"
            raise ValueError(msg)
        # self.fig.canvas.flush_events()
        self.fig.canvas.draw_idle()

    def add_data(self, item: ds.DataItem) -> None:
        if len(self.state["x"]) == 0:
            # first plot
            self._axis_setup(item)

        # update x-state
        if self.x_name == "round":
            round_nr = len(self.state["x"])
            self.state["x"] += [round_nr]
        else:
            self.state["x"] += [item[self.x_name]]
            # this may fail if x is not a key

        # update y-state
        if self.y_names is None:
            msg = "this should not happen: self.y_names must be set"
            raise ValueError(msg)
        for k in self.y_names:
            self.state["y"][k] += [item[k]]

        # update figure data
        m = float("inf")
        M = -float("inf")  # noqa: N806
        for k in self.y_names:
            self.lines[k].set_data(self.state["x"], self.state["y"][k])
            finite_values = [v for v in self.state["y"][k] if is_finite(v)]
            m = min([*finite_values, m])
            M = max([*finite_values, M])  # noqa: N806

        # rezoom
        if self.ax is None:
            msg = "this should not happen: self.ax must be set"
            raise ValueError(msg)
        self.ax.set_xlim(self.state["x"][0] - 1, self.state["x"][-1] + 1)
        y_offset = min(M - m, 1.0) * 0.1
        y_lower = m - y_offset if is_finite(m - y_offset) else -1.0
        y_upper = M + y_offset if is_finite(M + y_offset) else 1.0
        self.ax.set_ylim(y_lower, y_upper)

        # draw
        if self.fig is None:
            msg = "this should not happen: self.fig must be set"
            raise ValueError(msg)
        # self.fig.canvas.flush_events()
        self.fig.canvas.draw_idle()

    def f(self, item: ds.DataItem) -> ds.DataItem:
        if self.incremental:
            self.add_data(item)
        else:
            self.plot(item)
        return item


class DeepPlot(ds.FunctionTerm):
    fig: matplotlib.figure.Figure | None
    ax: matplotlib.axes.Axes | None
    then_fun: Callable[[Self, ds.DataItem], None] | None

    def __init__(self, *argv, **argk) -> None:
        super().__init__(*argv, **argk)
        self.lines = {}
        self.fig, self.ax = plt.subplots()

    def _axis_setup(self, params: PlotParams) -> None:  # noqa: ARG002
        if self.ax is None:
            msg = "this should not happen: self.ax must be set"
            raise ValueError(msg)

        self.ax.set_title(self.name)

    def _call_plot_dicts(self, d: dict) -> None:
        if isinstance(d, dict):
            for k in d:
                if k == "plot":
                    self.plot(d["plot"])
                elif k == "plot-list":
                    for v in d["plot-list"]:
                        self.plot(v)
                else:
                    self._call_plot_dicts(d[k])

    def plot(self, params: PlotParams) -> None:
        self._axis_setup(params)

        # draw
        if self.fig is None:
            msg = "this should not happen: self.fig must be set"
            raise ValueError(msg)

        if self.ax is None:
            msg = "this should not happen: self.ax must be set"
            raise ValueError(msg)

        if not isinstance(params, dict):
            msg = "expecting a plot dictionary"
            raise TypeError(msg)

        if "y" not in params:
            msg = "expected key 'y' in a plot dict"
            raise ValueError(msg)
        y = params["y"]

        x = params["x"] if "x" in params else range(len(y))
        args = params["args"] if "args" in params else []
        kwargs = params["kwargs"] if "kwargs" in params else {}

        self.ax.plot(x, y, *args, **kwargs)
        self.fig.canvas.draw_idle()

    def f(self, item: ds.DataItem) -> ds.DataItem:
        # potentially clear the axis
        if self.ax is not None:
            self.ax.clear()

        # do the plotting
        self._call_plot_dicts(item)

        # potentially call the then-registered function
        if self.then_fun is not None:
            self.then_fun(self, item)

        # return all
        return item

    def then(self, fun: Callable[[Self, ds.DataItem], None]) -> Self:
        """run a function after the plotting"""
        self.then_fun = fun
        return self


def is_finite(num: float | np.number) -> bool:
    # try:
    return not (math.isnan(num) or math.isinf(num))
    # except Exception as e:
    #     print('error', num)

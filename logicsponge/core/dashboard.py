import logging  # noqa: D100
import math
import threading
import uuid
from collections.abc import Callable
from typing import Any, Self, TypedDict

import dash
import dash_bootstrap_components as dbc
import dash_svg as svg
import numpy as np
import plotly.colors
import plotly.graph_objs as go
from dash import ClientsideFunction, clientside_callback, dcc, html
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots

import logicsponge.core as ls

logger = logging.getLogger(__name__)


class PlotParams(TypedDict):
    """Parameters of a plot."""

    x: list[float]
    y: list[float]
    args: list
    kwargs: dict


class Line(TypedDict):
    """Parameters of a line."""

    x: list[float]
    y: list[float]
    label: str
    style: dict[str, Any]


def hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    """Convert hex to rgb."""
    hex_color = hex_color.lstrip("#")
    return tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))  # type: ignore  # noqa: PGH003


class Graph:
    """A graph in the dashboard."""

    lines: list[Line]
    name: str
    uuid: str
    shapes: list[dict[str, Any]]
    stacked: bool
    log_y: bool
    range_y: list[float] | None

    def __init__(
        self,
        name: str,
        stacked: bool = False,  # noqa: FBT001, FBT002
        log_y: bool = False,  # noqa: FBT001, FBT002
        range_y: list[float] | None = None,
    ) -> None:
        """Create a Graph object.

        Args:
            name (str): The name.
            stacked (bool): If plotted lines are stacked as subgraphs instead of being plotted as overlays.
            log_y (bool): If y-axis is log.
            range_y (list[float], optional): The range of the y-axis.

        """
        with lock:
            self.uuid = str(uuid.uuid4())
            self.name = name
            self.lines = []
            self.shapes = []
            self.stacked = stacked
            self.log_y = log_y
            self.range_y = range_y

    def clear(self) -> None:
        """Clear the plot."""
        with lock:
            self.lines = []

    def add_line(
        self, x: list[float], y: list[float], label: str | None = None, style: dict[str, Any] | None = None
    ) -> None:
        """Add a line to the plot."""
        with lock:
            new_label = label if label is not None else str(len(self.lines))
            new_line: Line = {"x": x, "y": y, "label": new_label, "style": {} if style is None else style}
            self.lines.append(new_line)

    def get_line(self, label: str) -> Line | None:
        """Get a line of the plot."""
        with lock:
            for line in self.lines:
                if line["label"] == label:
                    return line
        return None

    def append_to_line(self, label: str, x: float | None, y: float) -> None:
        """Append x,y to a line."""
        line = self.get_line(label)
        if line is None:
            msg = "line does not exist"
            raise ValueError(msg)

        with lock:
            if x is None:
                line["x"].append(len(line["x"]))
            else:
                line["x"].append(x)
            line["y"].append(y)

    def to_dcc_graph(self) -> dcc.Graph:
        """Get a navigable graph as a dcc.Graph object."""
        standard_colors = plotly.colors.qualitative.Plotly

        if not self.stacked:
            layout = go.Layout(
                title=self.name,
                xaxis={"title": "x"},
                yaxis={"title": "y", "type": "log" if self.log_y else "linear"},
                margin={"l": 40, "b": 80, "t": 80, "r": 80},
                hovermode="closest",
                uirevision=True,
                shapes=self.shapes,
            )
            scatter_objects = []
            for i, line in enumerate(self.lines):
                color = standard_colors[i % len(standard_colors)]
                rgb = hex_to_rgb(color)
                style_mode = line["style"].get("mode", "lines")
                if len(line["y"]) > 0 and isinstance(line["y"][0], list):
                    # multiple y values per x -> plot mean with errors
                    x = line["x"]
                    y = [np.mean(inner_list) for inner_list in line["y"]]
                    y_lower = [np.min(inner_list) for inner_list in line["y"]]
                    y_upper = [np.max(inner_list) for inner_list in line["y"]]
                    scatter_objects += [
                        go.Scatter(x=line["x"], y=y, mode=style_mode, name=line["label"], marker={"color": color}),
                        go.Scatter(
                            name="Upper Bound",
                            x=x,
                            y=y_upper,
                            mode="lines",
                            marker={"color": f"rgba({rgb[0]},{rgb[1]},{rgb[2]},0.1)"},
                            line={"width": 0},
                            showlegend=False,
                        ),
                        go.Scatter(
                            name="Lower Bound",
                            x=x,
                            y=y_lower,
                            marker={"color": f"rgba({rgb[0]},{rgb[1]},{rgb[2]},0.1)"},
                            line={"width": 0},
                            mode="lines",
                            fillcolor=f"rgba({rgb[0]},{rgb[1]},{rgb[2]},0.1)",
                            fill="tonexty",
                            showlegend=False,
                        ),
                    ]

                else:
                    # single y value per x
                    scatter_objects.append(go.Scatter(x=line["x"], y=line["y"], mode=style_mode, name=line["label"]))

            return dcc.Graph(
                id=f"graph-{self.uuid}",
                figure={
                    "data": scatter_objects,
                    "layout": layout,
                },
            )

        # otherwise stack
        fig = make_subplots(
            rows=len(self.lines), cols=1, shared_xaxes=True, subplot_titles=[line["label"] for line in self.lines]
        )
        for i, line in enumerate(self.lines):
            style_mode = line["style"].get("mode", "lines")
            style_line = line["style"].get("line", {"color": "black", "width": 1})
            fig.add_trace(
                go.Scatter(
                    x=line["x"],
                    y=line["y"],
                    mode=style_mode,
                    name=line["label"],
                    line=style_line,
                ),
                row=i + 1,
                col=1,
            )
        fig.update_layout(
            {
                "title": self.name,
                "margin": {"l": 40, "b": 80, "t": 80, "r": 80},
                "hovermode": "closest",
                "uirevision": True,
                "showlegend": False,
            }
        )
        fig.update_layout(
            {
                f"yaxis{i}": {"tickmode": "array", "tickvals": [False, True], "ticktext": ["0", "1"], "showgrid": False}
                for i in range(1, len(self.lines) + 1)
            }
        )
        fig.update_yaxes(type="category", categoryorder="array", categoryarray=[False, True], range=[False, True])

        return dcc.Graph(
            id=f"graph-{self.uuid}",
            figure=fig,
        )


class StatisticsGraph:
    """Graph with performance statistics."""

    circuit: ls.Term

    def __init__(self, circuit: ls.Term) -> None:
        """Create a StatisticsGraph object."""
        self.circuit = circuit

    @staticmethod
    def dfs(term: ls.Term, result: list[ls.Term]) -> None:
        """Run dfs on a Term and add to result list."""
        result.append(term)
        if isinstance(term, ls.SequentialTerm | ls.ParallelTerm):
            StatisticsGraph.dfs(term.term_left, result=result)
            StatisticsGraph.dfs(term.term_right, result=result)

    @property
    def term_list(self) -> list[ls.Term]:
        """Return a list of Terms."""
        result: list[ls.Term] = []
        StatisticsGraph.dfs(self.circuit, result=result)
        return result

    @property
    def stats_dict(self) -> dict:
        """Return a dict with statistics."""
        logger.debug("stats_dict")

        def map_fun(term: ls.Term) -> dict:
            if isinstance(term, ls.SourceTerm | ls.FunctionTerm):
                return {term.id: term.stats}
            return {}

        filtered = [term for term in self.term_list if isinstance(term, ls.SourceTerm | ls.FunctionTerm)]
        mapped = list(map(map_fun, filtered))
        merged_dict = {}
        for d in mapped:
            merged_dict.update(d)
        logger.debug("stats_dict done")
        return merged_dict


# state of the dashboard
external_scripts: list[str | dict[str, Any]] | None = [
    "https://d3js.org/d3.v5.min.js",
    "https://unpkg.com/@popperjs/core@2",
    "https://unpkg.com/tippy.js@6",
    "assets/dagre-d3.js",
]
app = dash.Dash(
    __name__,
    title="logicsponge",
    update_title=None,  # type: ignore  # noqa: PGH003
    suppress_callback_exceptions=True,
    external_scripts=external_scripts,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)
graphs: list[Graph] = []
statistics_graph: StatisticsGraph | None = None
lock = threading.Lock()

SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "16rem",
    "padding": "2rem 1rem",
    "backgroundColor": "#f8f9fa",
}

CONTENT_STYLE = {
    "marginLeft": "18rem",
    "marginRight": "2rem",
    "padding": "2rem 1rem",
}

sidebar = html.Div(
    [
        html.H2("logicsponge", className="display-6"),
        html.Hr(),
        dbc.Nav(
            [
                dbc.NavLink("Plotting", href="/", active="exact"),
                dbc.NavLink("Sponge", href="/sponge", active="exact"),
                dbc.NavLink("Latencies", href="/latencies", active="exact"),
            ],
            vertical=True,
            pills=True,
        ),
    ],
    style=SIDEBAR_STYLE,
)

content = html.Div(id="page-content", style=CONTENT_STYLE)

app.layout = html.Div([dcc.Location(id="url"), sidebar, content])

page_graphs = html.Div(
    [
        html.Div(id="graphs-container"),
        dcc.Interval(
            id="interval-graphs",
            interval=2 * 1000,  # in milliseconds
            n_intervals=0,
        ),
    ]
)

page_stats = html.Div(
    [
        dcc.Store(id="stats-data"),
        html.Div(
            [
                svg.Svg(id="stats-svg", children=svg.G(id="stats-g")),
            ],
            id="stats-container",
        ),
        dcc.Interval(
            id="interval-stats",
            interval=2 * 1000,  # in milliseconds
            n_intervals=0,
        ),
    ]
)

page_latencies = html.Div(
    [
        html.Div(id="latencies-container"),
        dcc.Interval(
            id="interval-latencies",
            interval=2 * 1000,  # in milliseconds
            n_intervals=0,
        ),
    ]
)


def register_graph(graph: Graph) -> None:
    """Register a new graph."""
    with lock:
        graphs.append(graph)


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname: str) -> html.Div:
    """Call to render the page content."""
    if pathname == "/":
        return page_graphs
    if pathname == "/sponge":
        return page_stats
    if pathname == "/latencies":
        return page_latencies

    return html.Div(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ],
        className="p-3 bg-light rounded-3",
    )


@app.callback(Output("graphs-container", "children"), [Input("interval-graphs", "n_intervals")])
def update_graphs(n: int) -> list[html.Div]:  # noqa: ARG001
    """Call to update the layout with all graphs."""
    logger.debug("update_graphs")
    with lock:
        graphs_html = []
        for graph in graphs:
            graphs_html.append(  # noqa: PERF401
                html.Div(
                    [graph.to_dcc_graph()],
                    className="graph",
                )
            )
        return graphs_html


@app.callback(Output("latencies-container", "children"), [Input("interval-latencies", "n_intervals")])
def update_latencies(n: int) -> html.Div:  # noqa: ARG001
    """Call to update the latencies."""
    logger.debug("update_latencies")
    if statistics_graph is None:
        return html.Div([])

    statistics = statistics_graph.stats_dict
    y = [term["name"] for term in statistics.values()]
    x = [term["output"]["latency_max"] for term in statistics.values()]
    x = [0 if math.isnan(v) else v for v in x]  # nan -> 0
    sorted_pairs = sorted(zip(x, y, strict=False), key=lambda pair: pair[0])
    sorted_x = [pair[0] for pair in sorted_pairs]
    sorted_y = [pair[1] for pair in sorted_pairs]
    sorted_y = [f"{len(sorted_y) - i}: {v}" for i, v in enumerate(sorted_y)]
    return html.Div(
        [
            dcc.Graph(
                id="graph-latencies",
                figure={
                    "data": [
                        {
                            "y": sorted_y[-20:],
                            "x": sorted_x[-20:],
                            "type": "bar",
                            "orientation": "h",
                        }
                    ],
                    "layout": go.Layout(
                        xaxis={"autorange": True, "automargin": True},
                        yaxis={"autorange": True, "automargin": True},
                        title="Top 20 max latencies [s]",
                        hovermode="closest",
                        uirevision=True,
                    ),
                },
            ),
        ],
    )


clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="reset_first"),
    Input("url", "pathname"),
)

clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="draw_term_stats"),
    Input("stats-data", "data"),
)


# Callback
@app.callback(Output("stats-data", "data"), [Input("interval-stats", "n_intervals")])
def update_stats(n: int) -> dict:  # noqa: ARG001
    """Call to update statistics."""
    if statistics_graph is not None:
        return statistics_graph.stats_dict
    return {}


class Plot(ls.FunctionTerm):
    """Plot data items as they arrive.

    Typical uses are:
    - Plot(x='a', y=['b', 'c'])
    - Plot(x='a', y='b')
    - Plot(y='b') : plot over round number
    - Plot() : plot all keys over round number
    """

    x_name: str
    y_names: list[str] | None
    graph: Graph | None
    stacked: bool
    style: dict[str, Any]
    log_y: bool
    range_y: list[float] | None

    def __init__(  # noqa: PLR0913
        self,
        *args,  # noqa: ANN002
        x: str = "round",
        y: str | list[str] | None = None,
        stacked: bool = False,
        style: dict[str, Any] | None = None,
        log_y: bool = False,
        range_y: list[float] | None = None,
        **kwargs,  # noqa: ANN003
    ) -> None:
        """Create a new Plot object."""
        super().__init__(*args, **kwargs)
        self.x_name = x
        if isinstance(y, str):
            self.y_names = [y]
        else:
            self.y_names = y
        self.graph = None
        self.stacked = stacked
        self.style = {} if style is None else style
        self.log_y = log_y
        self.range_y = range_y

    def _axis_setup(self, item: ls.DataItem) -> None:
        # check if need to discover the y_names
        if self.y_names is None:
            # we will plot all keys of the input
            # except for the x-key
            self.y_names = list(set(item.keys()) - {self.x_name})

        self.graph = Graph(self.name, stacked=self.stacked, log_y=self.log_y, range_y=self.range_y)
        register_graph(self.graph)
        for y_name in self.y_names:
            self.graph.add_line(label=y_name, x=[], y=[], style=self.style)

    def add_data(self, item: ls.DataItem) -> None:
        """Happening on adding new data."""
        if self.graph is None:
            # first plot
            self._axis_setup(item)

        if self.y_names is None:
            msg = "this should not happen: self.y_names must be set"
            raise ValueError(msg)

        try:
            x = None if self.x_name == "round" else item[self.x_name]
        except KeyError as err:
            msg = (
                f"Term with name '{self.name}': "
                f"The x-value was expected to be key '{self.x_name}' which does not exist. "
                f"The keys I found are {list(item.keys())}."
            )
            raise KeyError(msg) from err

        try:
            for k in self.y_names:
                y = item[k]
                if self.graph is None:
                    msg = "self.graph should be set here"
                    raise ValueError(msg)
                self.graph.append_to_line(label=k, x=x, y=y)
        except KeyError as err:
            msg = (
                f"Term with name '{self.name}': Could not plot the key {err}. The keys I found are {list(item.keys())}."
            )
            raise KeyError(msg) from err

    def f(self, item: ls.DataItem) -> ls.DataItem:
        """Add the new data."""
        self.add_data(item)
        return item


class BinaryPlot(Plot):
    """Plots a monitored binary signal."""

    x_name: str
    y_names: list[str] | None
    graph: Graph | None

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a BinaryPlot object."""
        super().__init__(*args, **kwargs)

    def _axis_setup(self, item: ls.DataItem) -> None:
        # check if need to discover the y_names
        if self.y_names is None:
            # we will plot all keys of the input
            # except for the x-key
            self.y_names = list(set(item.keys()) - {self.x_name})

        self.graph = Graph(self.name, stacked=True)
        register_graph(self.graph)
        for y_name in self.y_names:
            self.graph.add_line(label=y_name, x=[], y=[])

    def f(self, item: ls.DataItem) -> ls.DataItem:
        """Execute on new data."""
        self.add_data(item)
        return item


class DeepPlot(ls.FunctionTerm):
    """A DeepPlot is used to plot a complete graph."""

    then_fun: Callable[[Self, ls.DataItem], None] | None
    graph: Graph

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Create a DeepPlot object."""
        super().__init__(*args, **kwargs)
        self.graph = Graph(self.name)
        register_graph(self.graph)

    def _axis_setup(self, params: PlotParams) -> None:
        pass

    def _call_plot_dicts(self, d: dict | ls.DataItem) -> None:
        if isinstance(d, dict):
            for k in d:
                if k == "plot":
                    self.plot_line(d["plot"])
                elif k == "plot-list":
                    for v in d["plot-list"]:
                        self.plot_line(v)
                else:
                    self._call_plot_dicts(d[k])

    def plot_line(self, params: PlotParams) -> None:
        """Plot a line."""
        self._axis_setup(params)

        # draw
        if not isinstance(params, dict):
            msg = "expecting a plot dictionary"
            raise TypeError(msg)

        if "y" not in params:
            msg = "expected key 'y' in a plot dict"
            raise ValueError(msg)
        y = params["y"]

        x = params["x"] if "x" in params else [float(i) for i in range(len(y))]
        # args = params["args"] if "args" in params else []
        kwargs = params.get("kwargs", {})

        label = kwargs.get("label", None)
        self.graph.add_line(x=x, y=y, label=label)

    def f(self, item: ls.DataItem) -> ls.DataItem:
        """Run the plotting."""
        self.graph.clear()
        self._call_plot_dicts(item)

        # potentially call the then-registered function
        if self.then_fun is not None:
            self.then_fun(self, item)

        # return all
        return item

    def then(self, fun: Callable[[Self, ls.DataItem], None]) -> Self:
        """Run a function after plotting."""
        self.then_fun = fun
        return self


def is_finite(num: float | np.number) -> bool:
    """Return if a number is finite."""
    return not (math.isnan(num) or math.isinf(num))


def show_stats(circuit: ls.Term) -> None:
    """Show statistics in the dashboard."""
    global statistics_graph  # noqa: PLW0603
    statistics_graph = StatisticsGraph(circuit)


def run(debug: bool = False) -> None:  # noqa: FBT001, FBT002
    """Run the dashboard app."""
    # NOTE: if debug=True then modules may load twice, and
    # this will be bad for running circuits etc.
    if debug:
        logger.warning("Enabling debugging may lead to >1 execution of module code.")
    app.run(debug=debug)

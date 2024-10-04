import logging
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

import datasponge.core as ds

logger = logging.getLogger(__name__)


class PlotParams(TypedDict):
    x: list[float]
    y: list[float]
    args: list
    kwargs: dict


class Line(TypedDict):
    x: list[float]
    y: list[float]
    label: str


def hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    hex_color = hex_color.lstrip("#")
    return tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))  # type: ignore


class Graph:
    lines: list[Line]
    name: str
    uuid: str
    shapes: list[dict[str, Any]]
    stacked: bool

    def __init__(self, name: str, stacked: bool = False) -> None:  # noqa: FBT001, FBT002
        """
        stagged: if ploted lines are stacked as subgraphs instead of being plotted as overlays
        """
        with lock:
            self.uuid = str(uuid.uuid4())
            self.name = name
            self.lines = []
            self.shapes = []
            self.stacked = stacked

    def clear(self) -> None:
        with lock:
            self.lines = []

    def add_line(self, x: list[float], y: list[float], label: str | None = None) -> None:
        with lock:
            new_label = label if label is not None else str(len(self.lines))
            new_line: Line = {"x": x, "y": y, "label": new_label}
            self.lines.append(new_line)

    def get_line(self, label: str) -> Line | None:
        with lock:
            for line in self.lines:
                if line["label"] == label:
                    return line
        return None

    def append_to_line(self, label: str, x: float | None, y: float) -> None:
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
        standard_colors = plotly.colors.qualitative.Plotly

        if not self.stacked:
            layout = go.Layout(
                title=self.name,
                xaxis={"title": "x"},
                yaxis={"title": "y"},
                margin={"l": 40, "b": 80, "t": 80, "r": 80},
                hovermode="closest",
                uirevision=True,
                shapes=self.shapes,
            )
            scatter_objects = []
            for i, line in enumerate(self.lines):
                color = standard_colors[i % len(standard_colors)]
                rgb = hex_to_rgb(color)
                if len(line["y"]) > 0 and isinstance(line["y"][0], list):
                    # muliple y values per x -> plot mean with errors
                    x = line["x"]
                    y = [np.mean(inner_list) for inner_list in line["y"]]
                    y_lower = [np.min(inner_list) for inner_list in line["y"]]
                    y_upper = [np.max(inner_list) for inner_list in line["y"]]
                    scatter_objects += [
                        go.Scatter(x=line["x"], y=y, mode="lines+markers", name=line["label"], marker={"color": color}),
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
                    scatter_objects.append(
                        go.Scatter(x=line["x"], y=line["y"], mode="lines+markers", name=line["label"])
                    )

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
            fig.add_trace(
                go.Scatter(
                    x=line["x"],
                    y=line["y"],
                    mode="lines+markers",
                    name=line["label"],
                    line={"color": "black", "width": 1},
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
    circuit: ds.Term

    def __init__(self, circuit: ds.Term) -> None:
        self.circuit = circuit

    @staticmethod
    def dfs(term: ds.Term, result: list[ds.Term]) -> None:
        result.append(term)
        if isinstance(term, ds.SequentialTerm | ds.ParallelTerm):
            StatisticsGraph.dfs(term.term_left, result=result)
            StatisticsGraph.dfs(term.term_right, result=result)

    @property
    def term_list(self) -> list[ds.Term]:
        result = []
        StatisticsGraph.dfs(self.circuit, result=result)
        return result

    @property
    def stats_dict(self) -> dict:
        logging.debug("stats_dict")

        def map_fun(term: ds.Term) -> dict:
            if isinstance(term, ds.SourceTerm | ds.FunctionTerm):
                return {term.id: term.stats}
            return {}

        filtered = [term for term in self.term_list if isinstance(term, ds.SourceTerm | ds.FunctionTerm)]
        mapped = list(map(map_fun, filtered))
        merged_dict = {}
        for d in mapped:
            merged_dict.update(d)
        logging.debug("stats_dict done")
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
    title="Circuits",
    update_title=None,  # type: ignore
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
        html.H2("Circuits", className="display-4"),
        html.Hr(),
        dbc.Nav(
            [
                dbc.NavLink("Graphs", href="/", active="exact"),
                dbc.NavLink("Stats", href="/stats", active="exact"),
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
    with lock:
        graphs.append(graph)


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    if pathname == "/":
        return page_graphs
    if pathname == "/stats":
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


# Callback to update the layout with all graphs
@app.callback(Output("graphs-container", "children"), [Input("interval-graphs", "n_intervals")])
def update_graphs(n):  # noqa: ARG001
    logger.info("update_graphs")
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
def update_latencies(n):  # noqa: ARG001
    logger.info("update_latencies")
    if statistics_graph is None:
        return html.Div([])

    statistics = statistics_graph.stats_dict
    y = [term["name"] for term in statistics.values()]
    x = [term["output"]["latency_max"] for term in statistics.values()]
    x = [0 if math.isnan(v) else v for v in x]  # nan -> 0
    sorted_pairs = sorted(zip(x, y, strict=False), key=lambda pair: pair[0])
    sorted_x = [pair[0] for pair in sorted_pairs]
    sorted_y = [pair[1] for pair in sorted_pairs]
    sorted_y = [f"{len(sorted_y)-i}: {v}" for i, v in enumerate(sorted_y)]
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
def update_stats(n):  # noqa: ARG001
    if statistics_graph is not None:
        return statistics_graph.stats_dict
    return {}


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
    graph: Graph | None
    stacked: bool

    def __init__(
        self, *argv, x: str = "round", y: str | list[str] | None = None, stacked: bool = False, **argk
    ) -> None:
        super().__init__(*argv, **argk)
        self.x_name = x
        if isinstance(y, str):
            self.y_names = [y]
        else:
            self.y_names = y
        self.graph = None
        self.stacked = stacked

    def _axis_setup(self, item: ds.DataItem) -> None:
        # check if need to discover the y_names
        if self.y_names is None:
            # we will plot all keys of the input
            # except for the x-key
            self.y_names = list(set(item.keys()) - {self.x_name})

        self.graph = Graph(self.name, stacked=self.stacked)
        register_graph(self.graph)
        for y_name in self.y_names:
            self.graph.add_line(label=y_name, x=[], y=[])

    def add_data(self, item: ds.DataItem) -> None:
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
                f"Term with name '{self.name}': "
                f"Could not plot the key {err}. "
                f"The keys I found are {list(item.keys())}."
            )
            raise KeyError(msg) from err

    def f(self, item: ds.DataItem) -> ds.DataItem:
        self.add_data(item)
        return item


class BinaryPlot(Plot):
    """plots monitored binary signal"""

    x_name: str
    y_names: list[str] | None
    graph: Graph | None

    def __init__(self, *argv, **argk) -> None:
        super().__init__(*argv, **argk)

    def _axis_setup(self, item: ds.DataItem) -> None:
        # check if need to discover the y_names
        if self.y_names is None:
            # we will plot all keys of the input
            # except for the x-key
            self.y_names = list(set(item.keys()) - {self.x_name})

        self.graph = Graph(self.name, stacked=True)
        register_graph(self.graph)
        for y_name in self.y_names:
            self.graph.add_line(label=y_name, x=[], y=[])

    def f(self, item: ds.DataItem) -> ds.DataItem:
        self.add_data(item)
        return item


class DeepPlot(ds.FunctionTerm):
    then_fun: Callable[[Self, ds.DataItem], None] | None
    graph: Graph

    def __init__(self, *argv, **argk) -> None:
        super().__init__(*argv, **argk)
        self.graph = Graph(self.name)
        register_graph(self.graph)

    def _axis_setup(self, params: PlotParams) -> None:
        pass

    def _call_plot_dicts(self, d: dict) -> None:
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
        kwargs = params["kwargs"] if "kwargs" in params else {}

        label = kwargs.get("label", None)
        self.graph.add_line(x=x, y=y, label=label)

    def f(self, item: ds.DataItem) -> ds.DataItem:
        # do the plotting
        self.graph.clear()
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


# show statistics
def show_stats(circuit: ds.Term) -> None:
    global statistics_graph  # noqa: PLW0603
    statistics_graph = StatisticsGraph(circuit)


# Run the dashboard app
def run(debug: bool = False) -> None:  # noqa: FBT001, FBT002
    # NOTE: if debug=True then modules may load twice, and
    # this will be bad for running circuits etc.
    if debug:
        logger.warning("Enabling debugging may lead to >1 execution of module code.")
    app.run(debug=debug)

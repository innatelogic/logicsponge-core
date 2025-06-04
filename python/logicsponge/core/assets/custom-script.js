// init callbacks
if (!window.dash_clientside) {
    window.dash_clientside = {};
}

// init statistics state
var render = new dagreD3.render();
var first = true;
var g = new dagreD3.graphlib.Graph();
g.setGraph({
    nodesep: 70,
    ranksep: 50,
    rankdir: "LR",
    marginx: 20,
    marginy: 20
});


function format_duration(duration_sec) {
    // formats a durection in [s] to a nicer str
    if (duration_sec >= 60) {
        // >= 1 min 
        const minutes = Math.floor(value / 60);
        const seconds = Math.floor(value % 60);
        return `${minutes} min ${seconds} s`;
    } else if (duration_sec >= 1) {
        // >= 1 s
        const duration_ms = duration_sec * 1000
        const seconds = Math.floor(duration_ms / 1000);
        const milliseconds = Math.floor(duration_ms % 1000);
        return `${seconds}.${milliseconds} s`;
    } else {
        const duration_ms = duration_sec * 1000
        const milliseconds = Math.round(duration_ms * 100) / 100;
        return `${milliseconds} ms`;
    }
}

function is_busy(inputs) {
    // returns if aterm is busy depending on its inputs
    if (inputs) {
        for (var input in inputs) {
            if (inputs[input].write > inputs[input].read) {
                return true;
            }
        }
        return false;
    }
    // sources are always busy here
    return true;
}

function draw(term_stats) {
    // draws the graph with the term statistics
    // console.log(term_stats);
    let svg = d3.select("#stats-svg");
    let inner = svg.select("#stats-g");
    if (render === undefined) {
        console.error("render is undefined");
        return;
    }
    if (g === undefined) {
        console.error("g is undefined");
        return;
    }

    for (var id in term_stats) {
        let term = term_stats[id];
        let className = is_busy(term.inputs) ? "running" : "stopped";
        g.setNode(term.output.id, {
            // labelType: "html",
            // label: html,
            label: term.name,
            rx: 5,
            ry: 5,
            padding: 10,
            class: `term ${className} ${term.type}`,
            height: 20,
        });

        if (term.inputs) {
            for (var input in term.inputs) {
                g.setEdge(input, term.output.id, {
                    //curve: d3.curveBasis
                    label: `${term.inputs[input].write} / ${term.inputs[input].read}`
                });
            }
        }
    }

    // render all
    inner.call(render, g);

    // add tooltips
    inner.selectAll("g.node")
        // .attr("title", function(v) { return get_tootltip(v, g.node(v).description) })
        .each(function(node_id) {
            let term_id = Object.keys(term_stats).find(key => term_stats[key].output.id === node_id);
            let term = term_stats[term_id];
            let term_type = term.type || "undefined";
            let latency_max = term.output.latency_max;
            let latency_avg = term.output.latency_avg;
            let content = `
                <ul class="node-tooltip">
                    <li>type: ${term_type}</li>
                    <li>writes: ${term.output.write}</li>
                    <li>latency (avg): ${format_duration(latency_avg)}</li>
                    <li>latency (max): ${format_duration(latency_max)}</li>
                </ul>`;
            tippy(this, {
                content: content,
                placement: 'bottom',
                theme: 'light',
                arrow: true,
                allowHTML: true,
            }); 
        });
}

function handleZoom() {
    let svg = d3.select("#stats-svg");
    let inner = svg.select("#stats-g");
    inner.attr('transform', d3.event.transform);
}

function fit() {
    let svg = d3.select("#stats-svg");
    // let inner = svg.select("#stats-g");
    console.log("fit(): setup the zoom");

    // Zoom and scale to fit
    var graphWidth = g.graph().width + 30;
    var graphHeight = g.graph().height + 30;
    var width = parseInt(svg.style("width").replace(/px/, ""));
    var height = parseInt(svg.style("height").replace(/px/, ""));
    var zoomScale = Math.min(width / graphWidth, height / graphHeight);
    var translateX = (width / 2) - ((graphWidth * zoomScale) / 2)
    var translateY = (height / 2) - ((graphHeight * zoomScale) / 2);

    zoom = d3.zoom().on('zoom', handleZoom);
    svg.transition().duration(100).call(zoom.transform, d3.zoomIdentity.translate(translateX, translateY).scale(zoomScale));
    svg.call(zoom);
}

window.dash_clientside.clientside = {
    reset_first: function (term_stats) {
        console.log("reset_first()");
        first = true;
    },
    draw_term_stats: function (term_stats) {
        console.log("draw_term_stats()");
        draw(term_stats);
        if (first) {
            if (g === undefined) {
                console.error("g is undefined");
                return;
            }
            document.getElementById('stats-container').style.visibility = 'visible';
            fit();
            first = false;
        }
    }
}
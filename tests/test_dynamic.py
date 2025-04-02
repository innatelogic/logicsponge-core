# type: ignore

import logicsponge.core as ls


def test_create_dynamic():
    sponge = ls.DynamicSpawnTerm(filter_key="id", spawn_fun=lambda _: ls.Stop())
    sponge.start()
    sponge.join()


def test_run_dynamic():

    inputs = []
    class MySource(ls.SourceTerm):
        def run(self):
            for i in range(10):
                ds = ls.DataItem({"subject_id": i})
                self.output(ds)
                inputs.append(ds)

    class MyFunction(ls.FunctionTerm):
        def f(self, di: ls.DataItem) -> ls.DataItem:
            return ls.DataItem({**di, "treated": True})

    dyn_spawn = ls.DynamicSpawnTerm(filter_key="subject_id", spawn_fun=lambda _: ls.Id())

    outputs = []
    sink = ls.Dump(print_fun=lambda di: outputs.append(di))

    sponge = MySource() * dyn_spawn * sink
    sponge.start()
    sponge.join()


    assert set(outputs) == set(inputs)

# type: ignore

import logicsponge.core as ls


def test_create_dynamic():
    sponge = ls.DynamicForkTerm(filter_key="id", spawn_fun=lambda _: ls.Stop())
    sponge.start()


def test_run_dynamic():
    class MySource(ls.SourceTerm):
        def run(self):
            for i in range(10):
                self.output(ls.DataItem({"subject_id": i, "data": 0}))

    class MyFunction(ls.FunctionTerm):
        def f(self, di: ls.DataItem) -> ls.DataItem:
            return ls.DataItem({**di, "treated": True})

    fork = ls.DynamicForkTerm(filter_key="subject_id", spawn_fun=lambda _: MyFunction())

    outputs = []
    sink = ls.Dump(print_fun=lambda ds: outputs.append(ds[-1]))

    sponge = MySource() * fork * sink
    sponge.start()

    print(outputs)


if __name__ == "__main__":
    test_create_dynamic()

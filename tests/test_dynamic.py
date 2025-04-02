# type: ignore

import logicsponge.core as ls


def test_create_dynamic():
    sponge = ls.DynamicSpawnTerm(filter_key="id", spawn_fun=lambda _: ls.Stop())
    sponge.start()
    sponge.join()


def test_run_dynamic():
    # class MyPrint(ls.Print):
    #     def exit(self):
    #         print(self.id, self._output)

    inputs = []

    class MySource(ls.SourceTerm):
        def run(self):
            for i in range(10):
                ds = ls.DataItem({"subject_id": i})
                self.output(ds)
                inputs.append(ds)

    dyn_spawn = ls.DynamicSpawnTerm(filter_key="subject_id", spawn_fun=lambda _: ls.Id())

    outputs = []
    sink = ls.Dump(print_fun=lambda di: outputs.append(di))

    sponge = MySource() * dyn_spawn * sink
    sponge.start()
    sponge.join()

    assert set(inputs) == set(outputs), f"\n{inputs}\n{outputs}"


def main():
    test_run_dynamic()


if __name__ == "__main__":
    main()

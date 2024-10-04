# type: ignore

import pytest

import datasponge.core as ds


def test_upper():
    assert "foo".upper() == "FOO"


def test_isupper():
    assert "FOO".isupper()
    assert not "Foo".isupper()


def test_split():
    s = "hello world"
    assert s.split() == ["hello", "world"]
    # check that s.split fails when the separator is not a string
    with pytest.raises(TypeError):
        s.split(2)


def test_parallel():
    x = ds.FunctionTerm(name="x")
    y = ds.FunctionTerm(name="y")
    z = ds.FunctionTerm(name="z")
    u = x | y | z
    assert str(u) == "(Term(x) | Term(y) | Term(z))"
    assert type(u) is ds.ParallelTerm


def test_sequential():
    x = ds.FunctionTerm("x")
    y = ds.FunctionTerm("y")
    z = ds.FunctionTerm("z")
    u = x * y * z
    assert str(u) == "(Term(x); Term(y); Term(z))"
    assert type(u) is ds.SequentialTerm


def test_both():
    x = ds.FunctionTerm("x")
    y = ds.FunctionTerm("y")
    z = ds.FunctionTerm("z")
    a = x | y | z

    u = ds.FunctionTerm("u")
    v = ds.FunctionTerm("v")

    r = u * a * v

    assert str(r) == "(Term(u); (Term(x) | Term(y) | Term(z)); Term(v))"
    assert type(r) is ds.SequentialTerm


def test_n():
    """
    check if n graph works
    """
    x = ds.FunctionTerm("x")
    y = ds.FunctionTerm("y")
    a = ds.FunctionTerm("a")
    b = ds.FunctionTerm("b")

    term1 = (x | y) * a
    term2 = y * b
    term = term1 | term2

    assert str(term) == "(((Term(x) | Term(y)); Term(a)) | (Term(y); Term(b)))"


def test_n_reverse():
    """
    check if n graph works
    """
    x = ds.FunctionTerm("x")
    y = ds.FunctionTerm("y")
    a = ds.FunctionTerm("a")
    b = ds.FunctionTerm("b")

    term1 = a * (x | y)
    term2 = b * y
    term = term1 | term2

    assert str(term) == "((Term(a); (Term(x) | Term(y))) | (Term(b); Term(y)))"


def test_run_dataitem():
    class Hu(ds.FunctionTerm):
        def f(self, a: ds.DataItem):
            return a

    x = Hu(name="x")
    assert str(x) == "Term(x)"

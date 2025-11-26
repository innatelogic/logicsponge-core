"""Test terms."""

import pytest

import logicsponge.core as ls


def test_upper() -> None:
    """Simple upper test."""
    assert "foo".upper() == "FOO"


def test_isupper() -> None:
    """Simple upper test."""
    assert "FOO".isupper()
    assert not "Foo".isupper()


def test_split() -> None:
    """Simple split test."""
    s = "hello world"
    assert s.split() == ["hello", "world"]
    # check that s.split fails when the separator is not a string
    with pytest.raises(TypeError):
        s.split(2)  # type: ignore  # noqa: PGH003


def test_parallel() -> None:
    """Test parallel composition."""
    x = ls.FunctionTerm(name="x")
    y = ls.FunctionTerm(name="y")
    z = ls.FunctionTerm(name="z")
    u = x | y | z
    assert str(u) == "(Term(x) | Term(y) | Term(z))"
    assert type(u) is ls.ParallelTerm


def test_sequential() -> None:
    """Test sequential composition."""
    x = ls.FunctionTerm("x")
    y = ls.FunctionTerm("y")
    z = ls.FunctionTerm("z")
    u = x * y * z
    assert str(u) == "(Term(x); Term(y); Term(z))"
    assert type(u) is ls.SequentialTerm


def test_sequential_source() -> None:
    """Test sequential source composition."""
    s = ls.ConstantSourceTerm([], name="s")
    x = ls.Id("x")
    y = ls.Id("y")
    z = ls.Id("z")
    u = s * x * y * z
    assert str(u) == "(Term(s); Term(x); Term(y); Term(z))"
    assert type(u) is ls.SequentialTerm


def test_both() -> None:
    """Test compositions."""
    x = ls.FunctionTerm("x")
    y = ls.FunctionTerm("y")
    z = ls.FunctionTerm("z")
    a = x | y | z

    u = ls.FunctionTerm("u")
    v = ls.FunctionTerm("v")

    r = u * a * v

    assert str(r) == "(Term(u); (Term(x) | Term(y) | Term(z)); Term(v))"
    assert type(r) is ls.SequentialTerm


def test_n() -> None:
    """Check if n graph works."""
    x = ls.FunctionTerm("x")
    y = ls.FunctionTerm("y")
    a = ls.FunctionTerm("a")
    b = ls.FunctionTerm("b")

    term1 = (x | y) * a
    term2 = y * b
    term = term1 | term2

    assert str(term) == "(((Term(x) | Term(y)); Term(a)) | (Term(y); Term(b)))"


def test_n_reverse() -> None:
    """Check if n graph works."""
    x = ls.FunctionTerm("x")
    y = ls.FunctionTerm("y")
    a = ls.FunctionTerm("a")
    b = ls.FunctionTerm("b")

    term1 = a * (x | y)
    term2 = b * y
    term = term1 | term2

    assert str(term) == "((Term(a); (Term(x) | Term(y))) | (Term(b); Term(y)))"


def test_run_dataitem() -> None:
    """Test creating a Term."""

    class Hu(ls.FunctionTerm):
        def f(self, di: ls.DataItem) -> ls.DataItem:
            return di

    x = Hu(name="x")
    assert str(x) == "Term(x)"


def test_term_accepts_any_name() -> None:
    """Term constructors accept any value as name (no runtime validation)."""

    class Foo(ls.FunctionTerm):
        def f(self, di: ls.DataItem) -> ls.DataItem:
            return di

    # No runtime validation - accepts any type
    term = Foo(True)  # type: ignore[arg-type]  # noqa: FBT003
    assert term.name == True  # noqa: E712


def test_term_rejects_multiple_positional_args() -> None:
    """Term constructors should allow at most one positional argument."""

    class Bar(ls.FunctionTerm):
        def f(self, di: ls.DataItem) -> ls.DataItem:
            return di

    with pytest.raises(TypeError):
        Bar("name", "extra")  # type: ignore[arg-type]

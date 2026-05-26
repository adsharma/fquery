import pytest

pytest.importorskip("ladybug")
pytest.importorskip("pyarrow")


def test_ladybug_demo_save_path():
    from examples.ladybug_demo import save_demo

    save_demo()


def test_ladybug_demo_arrow_memory_path():
    from examples.ladybug_demo import arrow_memory_demo

    arrow_memory_demo()

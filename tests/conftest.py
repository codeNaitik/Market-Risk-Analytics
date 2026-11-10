import os

import pytest


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    from mra.processing.spark_session import get_spark

    session = get_spark("tests")
    yield session
    session.stop()

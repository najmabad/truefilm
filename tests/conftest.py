import pytest

import pyspark.sql.types as t

from truefilm_etl.utils.spark import MySparkSession


@pytest.fixture
def spark():
    """Fixture that returns an instance of the FileDAO class."""
    return MySparkSession().spark


@pytest.fixture
def schema():
    """Fixture that returns the schema for the XML data."""
    return t.StructType(
        [
            t.StructField("abstract", t.StringType(), True),
            t.StructField(
                "links",
                t.StructType(
                    [
                        t.StructField(
                            "sublink",
                            t.ArrayType(
                                t.StructType(
                                    [
                                        t.StructField(
                                            "_linktype", t.StringType(), True
                                        ),
                                        t.StructField("anchor", t.StringType(), True),
                                        t.StructField("link", t.StringType(), True),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        )
                    ]
                ),
                True,
            ),
            t.StructField("title", t.StringType(), True),
            t.StructField("url", t.StringType(), True),
        ]
    )

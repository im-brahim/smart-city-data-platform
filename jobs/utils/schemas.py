from pyspark.sql.types import (      #type: ignore
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType, BooleanType, ArrayType
)

# Mirrors raw Bronze weather JSON shape.
# rain/snow are optional in the source API (only present when it's
# actually raining/snowing) — declaring them here means Spark always
# exposes the column, filling null when the raw file doesn't have it,
# instead of crashing or silently omitting the field depending on batch content.
WEATHER_SCHEMA = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType()),
    ])),
    StructField("weather", ArrayType(StructType([
        StructField("main", StringType()),
        StructField("description", StringType()),
    ]))),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", IntegerType()),
        StructField("pressure", IntegerType()),
    ])),
    StructField("visibility", IntegerType()),
    StructField("wind", StructType([
        StructField("speed", DoubleType()),
        StructField("deg", IntegerType()),
    ])),
    StructField("clouds", StructType([
        StructField("all", IntegerType()),
    ])),
    StructField("rain", StructType([
        StructField("1h", DoubleType()),
    ])),
    StructField("snow", StructType([
        StructField("1h", DoubleType()),
    ])),
    StructField("dt", LongType()),
    StructField("name", StringType()),
    StructField("ingested_at", StringType()),
])

# Mirrors raw Bronze traffic JSON shape.
TRAFFIC_SCHEMA = StructType([
    StructField("flowSegmentData", StructType([
        StructField("frc", StringType()),
        StructField("currentSpeed", IntegerType()),
        StructField("freeFlowSpeed", IntegerType()),
        StructField("currentTravelTime", IntegerType()),
        StructField("freeFlowTravelTime", IntegerType()),
        StructField("confidence", DoubleType()),
        StructField("roadClosure", BooleanType()),
        StructField("coordinates", StructType([
            StructField("coordinate", ArrayType(StructType([
                StructField("latitude", DoubleType()),
                StructField("longitude", DoubleType()),
            ])))
        ])),
    ])),
    StructField("ingested_at", StringType()),
])
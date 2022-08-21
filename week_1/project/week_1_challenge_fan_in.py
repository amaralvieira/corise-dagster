import csv
from datetime import datetime
from heapq import nlargest
from typing import List

from dagster import (
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    Out,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: list):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    with open(context.op_config["s3_key"]) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            stock = Stock.from_list(row)
            output.append(stock)
    return output


@op(
    config_schema={"nlargest": int},
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": DynamicOut(dagster_type=Aggregation)},
    tags={"kind": "python"},
    description="Determine the Stock with the greatest high value",
    )
def process_data(context, stocks):
    n = context.op_config["nlargest"]
    nhs = sorted(stocks, key=lambda x: x.high, reverse=True)[:n]

    for i, hs in enumerate(nhs):
        context.log.info(f'{i}: {hs.date} - {hs.high}')
        yield DynamicOutput(Aggregation(date=hs.date, high=hs.high),
              mapping_key=str(i),
              output_name='aggregation')

@op(
    ins={"aggregation": In(dagster_type=Aggregation)},
    )
def process_aggregations(aggregation):
    return aggregation

@op(
    ins={"aggregations": In(dagster_type=List[Aggregation])},
    )
def put_redis_data(aggregations):
    pass


@job
def week_1_pipeline():
    stocks = get_s3_data()
    aggregation = process_data(stocks)
    aggregations = aggregation.map(process_aggregations)
    put_redis_data(aggregations.collect())
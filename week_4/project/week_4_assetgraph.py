from typing import List

from dagster import op, graph, In, Out, Nothing, asset, AssetsDefinition, with_resources

from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={'s3'},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):

    s3 = context.resources.s3
    stocks = s3.get_data(context.op_config["s3_key"]) 

    return [Stock.from_list(stock) for stock in stocks]


@op(ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    tags={"kind": "python"},
    description="Determine the Stock with the greatest high value",)
def process_data(stocks):
    
    hs = sorted(stocks, key=lambda x: x.high, reverse=True)[0]

    return Aggregation(date=hs.date, high=hs.high)

@op(ins={"aggregation": In(dagster_type=Aggregation)},
    required_resource_keys={'redis'},
    tags={"kind": "redis"},
    description="Upload aggregations to Redis",)
def put_redis_data(context, aggregation):

    redis = context.resources.redis
    redis.put_data(str(aggregation.date), str(aggregation.high))

@graph
def week_4_graph():
    stocks = get_s3_data()
    aggregation = process_data(stocks)
    put_redis_data(aggregation)

week_4_asset = AssetsDefinition.from_graph(week_4_graph)

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://host.docker.internal:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

week_4_asset_docker = with_resources([week_4_asset], 
                                     resource_defs={"s3": s3_resource,
                                                    "redis": redis_resource},
                                     resource_config_by_key=docker)
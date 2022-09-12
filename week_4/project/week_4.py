from typing import List

from dagster import AssetIn, Nothing, asset, with_resources

from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    description="Get a list of stocks from an S3 file",
    compute_kind="s3",
    required_resource_keys={'s3'},
    config_schema={"s3_key": str},
    dagster_type=List[Stock],
    group_name='corise'
)
def get_s3_data(context):

    s3 = context.resources.s3
    stocks = s3.get_data(context.op_config["s3_key"]) 

    return [Stock.from_list(stock) for stock in stocks]


@asset(ins={"stocks": AssetIn('get_s3_data')},
       dagster_type=Aggregation,
       compute_kind= "python",
       description="Determine the Stock with the greatest high value",
       group_name='corise')
def process_data(stocks):
    
    hs = sorted(stocks, key=lambda x: x.high, reverse=True)[0]

    return Aggregation(date=hs.date, high=hs.high)

@asset(ins={"highest_stock": AssetIn('process_data')},
        required_resource_keys={'redis'},
        compute_kind="redis",
        description="Upload aggregations to Redis",
        group_name='corise')
def put_redis_data(context, highest_stock):

    redis = context.resources.redis
    redis.put_data(str(highest_stock.date), str(highest_stock.high))

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
#

assets_with_resources = with_resources([get_s3_data, process_data, put_redis_data], 
                                     resource_defs={"s3": s3_resource,
                                                    "redis": redis_resource},
                                     resource_config_by_key=docker['resources'])

get_s3_data_docker, process_data_docker, put_redis_data_docker = assets_with_resources
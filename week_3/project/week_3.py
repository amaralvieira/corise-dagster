from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
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
def week_3_pipeline():
    stocks = get_s3_data()
    aggregation = process_data(stocks)
    put_redis_data(aggregation)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
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


MONTHS = [ str(i+1) for i in range(10)]

@static_partitioned_config(partition_keys=MONTHS)
def docker_config(partition_key: str):
    filename = f'prefix/stock_{partition_key}.csv'
    
    return {**docker,
            **{"ops": {"get_s3_data": {"config": {"s3_key": filename}}}} }

local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10,delay=1),
)


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline  , cron_schedule="0 * * * *")


@sensor(job=docker_week_3_pipeline)
def docker_week_3_sensor(context):
    #https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#s3-sensors
    new_keys = get_s3_keys(bucket='dagster',
                           prefix='prefix',
                           endpoint_url='http://host.docker.internal:4566')
    if new_keys:
        for key in new_keys:
            yield RunRequest(
                run_key=key,
                run_config= { **docker,
                              **{"ops": {"get_s3_data": {"config": {"s3_key": key}}}} }
                )
    else:
        yield SkipReason(f"No new s3 files found in bucket.")
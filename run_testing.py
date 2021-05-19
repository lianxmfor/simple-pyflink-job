import json
import logging
import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    DataTypes,
    EnvironmentSettings,
    StreamTableEnvironment,
)
from pyflink.table.udf import udf


@udf(
    input_types=[DataTypes.STRING()],
    result_type=DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
)
def parse_content(content_str):
    if not isinstance(content_str, str):
        logging.exception("Cannot parse a non-string contentstr.")
        return {}
    res = {}
    try:
        content = json.loads(content_str)
        if not isinstance(content, dict):
            raise ValueError(
                f"Content string {content_str} cannot be parsed into a Python dictionary."
            )
        if "param" in content and not isinstance(content["param"], dict):
            raise ValueError(
                f'Content param {content["param"]} cannot be parsed into a Python dictionary.'
            )
        if "postId" in content:
            res["item_id"] = content["postId"]
        if "lid" in content:
            res["item_id"] = content["lid"]
        if "param" in content and "tag" in content["param"]:
            res["tag"] = content["param"]["tag"]
        # Align the udf result_type
        res = {str(k): str(v) for k, v in res.items()}
    except (json.JSONDecodeError, ValueError) as e:
        logging.exception("Failed to parse contentstr.")
    return res


SOURCE = """
CREATE TABLE kafka_source (
	`body` ROW<`log` ROW<`uid` BIGINT, serverts BIGINT, `contentstr` STRING>>
) WITH (
	'connector' = 'kafka',
	'topic' = 'source',
        'properties.bootstrap.servers' = '127.0.0.1:9092',
	'properties.group.id' = 'test-featurepipelines',
	'format' = 'json'
)
"""

SINK = """
CREATE TABLE kafka_sink (
	user_id BIGINT,
	datetime TIMESTAMP(3),
	features STRING,
	PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
	'connector' = 'upsert-kafka',
	'topic' = 'sink',
        'properties.bootstrap.servers' = '127.0.0.1:9092',
	'properties.group.id' = 'test-featurepipelines',
	'key.format' = 'json',
	'value.format' = 'json'
)
"""

TRANSFORM = """
INSERT INTO kafka_sink
WITH t1 AS (
    SELECT
        body['log']['uid'] user_id,
        PARSE_CONTENT(body['log']['contentstr']) content,
        body['log']['serverts'] server_ts
    FROM kafka_source
),
t2 AS (
    SELECT user_id, content['item_id'] item_id, content['tag'] tag, server_ts
    FROM t1
    WHERE content['item_id'] IS NOT NULL
    AND content['tag'] = '点击帖子卡片'
),
last_n AS (
    SELECT user_id, item_id, server_ts
    FROM (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY server_ts DESC) as row_num
        FROM t2)
    WHERE row_num <= 5
)
SELECT
    user_id,
    TO_TIMESTAMP(FROM_UNIXTIME(MAX(server_ts / 1000))) datetime,
    CONCAT('last_5_clicks=', LISTAGG(CAST(item_id AS STRING))) feature
FROM last_n
GROUP BY user_id
"""

exec_env = StreamExecutionEnvironment.get_execution_environment()
env_settings = EnvironmentSettings.Builder().use_blink_planner().build()
t_env = StreamTableEnvironment.create(
    stream_execution_environment=exec_env, environment_settings=env_settings
)

FAT_JAR_PATH = f"{os.getcwd()}/deps/flink-sql-connector-kafka_2.11-1.13.0.jar"
t_env.get_config().get_configuration().set_string(
    "pipeline.jars", f"file://{FAT_JAR_PATH}"
)

t_env.create_temporary_function("parse_content", parse_content)

t_env.execute_sql(SOURCE)
t_env.execute_sql(SINK)
t_result = t_env.execute_sql(TRANSFORM)

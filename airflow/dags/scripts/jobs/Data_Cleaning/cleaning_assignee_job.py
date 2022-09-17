import pyspark.sql.functions as f
import pyspark.sql.types as t

from dependencies.spark import start_spark
from dependencies.utils import extract_tsv_data, load_data_to_s3_as_parquet


def main():
    spark, log, config = start_spark(
        app_name="cleaning_assignee_job", files=["../configs/etl_config.json"]
    )

    log.warn("Cleaning Assignee Job is up and running")

    data = extract_tsv_data(spark, f"{config['raw_data_s3_path']}assignee.tsv")

    data_transformed = transform_data(data)

    load_data_to_s3_as_parquet(
        spark,
        data_transformed,
        path=f"{config['cleaned_data_s3_path']}assignee.parquet",
    )

    log.warn("Cleaning Assignee Job is finished")
    spark.stop()
    return None


def convert_type_to_string(assignee_type):
    if (
        assignee_type == 12
        or assignee_type == 2
        or assignee_type == 13
        or assignee_type == 3
    ):
        return "company"
    elif (
        assignee_type == 4
        or assignee_type == 5
        or assignee_type == 14
        or assignee_type == 15
    ):
        return "individual"
    elif (
        assignee_type == 6
        or assignee_type == 7
        or assignee_type == 8
        or assignee_type == 9
        or assignee_type == 16
        or assignee_type == 17
        or assignee_type == 18
        or assignee_type == 19
    ):
        return "government"
    else:
        "unknown"


def infer_type(type_string, name_first, name_last, organization):
    if type_string:
        return type_string
    else:
        if organization:
            return "company"
        elif name_first or name_last:
            return "individual"
        else:
            return "unknown"


def combine_name(name_first, name_last, organization):
    if organization:
        return organization
    elif name_first and name_last:
        return f"{name_first} {name_last}"
    elif name_last:
        return name_last
    elif name_first:
        return name_first
    else:
        return None


def transform_data(data):
    # Remove the unassigned
    transformed_data = data.filter((f.col("type") != 1))

    # Convert type from number to string
    convert_type_to_string_udf = f.udf(
        lambda entity_type: convert_type_to_string(entity_type), t.StringType()
    )
    transformed_data = transformed_data.withColumn(
        "type_string", convert_type_to_string_udf(f.col("type"))
    )

    # Infer assignee type
    infer_type_udf = f.udf(infer_type, t.StringType())
    transformed_data = transformed_data.withColumn(
        "type_string_cleaned",
        infer_type_udf(
            f.col("type_string"),
            f.col("name_first"),
            f.col("name_last"),
            f.col("organization"),
        ),
    )

    # Combine first name, last name, organization to a name column
    combine_name_udf = f.udf(combine_name, t.StringType())
    transformed_data = transformed_data.withColumn(
        "name",
        combine_name_udf(
            f.col("name_first"), f.col("name_last"), f.col("organization")
        ),
    )

    # Create new ID for every unique name and type
    transformed_data_with_id = (
        transformed_data.groupBy("name", "type_string_cleaned")
        .count()
        .withColumn(
            "new_id", f.sha2(f.concat(f.col("name"), f.col("type_string_cleaned")), 256)
        )
        .select(["name", "type_string_cleaned", "new_id"])
    )

    # Join the data with ID with the original data
    transformed_data_with_id = (
        transformed_data.join(
            transformed_data_with_id,
            (transformed_data.name == transformed_data_with_id.name)
            & (
                transformed_data.type_string_cleaned
                == transformed_data_with_id.type_string_cleaned
            ),
        )
        .drop(transformed_data_with_id.name)
        .drop(transformed_data_with_id.type_string_cleaned)
        .select(["id", "name", f.col("type_string_cleaned").alias("type"), "new_id"])
    )

    return transformed_data_with_id


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()

from pyspark.sql.functions import concat_ws, sha2, expr, col, lit, to_timestamp, transform, struct, to_json, flatten, when, size, array, regexp_extract
from scripts.utils.spark_utils.udfs import parse_price_range_udf

def process_google_data(spark, google_path):
    df_google = spark.read.json(google_path)
    df_google_normalized = df_google.select(
        expr("uuid()").alias("restaurant_id"),
        col("name").alias("restaurant_name"),
        col("avatar_url"),
        col("restaurant_type").alias("restaurant_description"),
        col("closing_time").alias("opening_hours"),
        to_json(parse_price_range_udf(col("price_range"))).alias("price_range"),
        col("address"),
        expr("search_results[0].geometry.location.lng").alias("longitude"),
        expr("search_results[0].geometry.location.lat").alias("latitude"),
        lit("GoogleMaps").alias("source"),
        col("restaurant_rating").cast("float").alias("restaurant_rating"),
        col("restaurant_rating_count").cast("int").alias("restaurant_rating_count"),
        col("reviews").alias("reviews"),
        flatten(col("reviews.photos")).alias("images"),
        lit(None).cast("string").alias("restaurant_url"),
        to_timestamp(col("crawl_time")).alias("crawl_time"),
        lit(None).cast("string").alias("crawl_id"),
        sha2(
            concat_ws(
                "|",
                col("name"),
                col("address")
            ),
            256
        ).alias("restaurant_hash")
    ).withColumn(
        "reviews",
        when(col("reviews").isNotNull() & (size(col("reviews")) > 0),
            transform(
                col("reviews"),
                lambda x: struct(
                    x.review_author.alias("review_author"),
                    x.review_date.alias("review_date"),
                    regexp_extract(x["rating"], r"(\d+(\.\d+)?)", 1).cast("float").alias("user_rating"),
                    x["text"].alias("user_review"),
                    sha2(
                        concat_ws(
                            "|",
                            x.review_author,
                            regexp_extract(x["rating"], r"(\d+(\.\d+)?)", 1),
                            x["text"]
                        ),
                        256
                    ).alias("review_hash"),
                    expr("uuid()").alias("review_id")
                )
            )).otherwise(array().cast("array<struct<review_author:string,review_date:string,user_rating:float,user_review:string,review_hash:string,review_id:string>>"))
    ).withColumn(
        "images",
        when(col("images").isNotNull() & (size(col("images")) > 0),
             transform(
                 col("images"),
                 lambda x: struct(
                     lit(None).alias("food_name"),
                     lit(None).alias("food_price"),
                     x.alias("img_url"),
                     sha2(
                        x,
                        256
                    ).alias("img_hash"),
                     expr("uuid()").alias("img_id")
                 )
             )).otherwise(array().cast("array<struct<food_name:string,food_price:string,img_url:string,img_hash:string,img_id:string>>"))
    )
    return df_google_normalized

def process_shopee_data(spark, shopee_path):
    # Triển khai logic xử lý dữ liệu Shopee (giả định)
    df_shopee = spark.read.json(shopee_path)
    # Thêm logic tương tự process_google_data để chuẩn hóa dữ liệu
    return df_shopee

def unify_data(df_shopee, df_google):
    # Triển khai logic hợp nhất dữ liệu từ Shopee và Google Maps
    # Ví dụ: hợp nhất dựa trên restaurant_hash hoặc các trường khóa khác
    return df_shopee.unionByName(df_google, allowMissingColumns=True)
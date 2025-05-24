import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# from configs.global_config import SELLECT_CRAWL_DATE_ARRAY
from scripts.etl.extract import get_unprocessed_files
from scripts.etl.transform import process_shopee_data, process_google_data, unify_data
from scripts.etl.load import save_processed_data_to_minIO, save_processed_data_to_local
from scripts.utils.minio_utils import init_minio_client
from scripts.utils.spark_utils.init_spark import init_spark

def main():
    # Khởi tạo MinIO
    client, bucket_name = init_minio_client()

    # Khởi tạo Spark
    spark = init_spark()

    date_str_array = ["2025-05-11"]
    for date_str in date_str_array:
        sources = ["shopeefood", "googlemaps"]
        all_dfs = {}

        print(f"Ngày hiện tại: {date_str}")
        for source in sources:
            files = get_unprocessed_files(client, bucket_name, source, date_str)
            dfs = []

            for file in files:
                path = f"s3a://{bucket_name}/raw/{source}/{date_str}/{file}"
                print(f"Processing {source} file: {path}")
                
                df = process_shopee_data(spark, path) if source == "shopeefood" else process_google_data(spark, path)
                if df.count() > 0:
                    dfs.append(df)

            if dfs:
                combined_df = dfs[0]
                for df in dfs[1:]:
                    combined_df = combined_df.unionByName(df)
                all_dfs[source] = combined_df

        # Chỉ unify nếu có ít nhất một nguồn có dữ liệu
        if "shopeefood" in all_dfs or "googlemaps" in all_dfs:
            df_shopee = all_dfs.get("shopeefood")
            df_google = all_dfs.get("googlemaps")

            # Nếu chỉ có một nguồn
            if df_shopee and not df_google:
                df_unified = df_shopee
            elif df_google and not df_shopee:
                df_unified = df_google
            else:
                df_unified = unify_data(df_shopee, df_google)

            print(f"Số bản ghi trong df_unified: {df_unified.count()}")
        else:
            print("Không có file mới để xử lý.")
            df_unified = None

        if df_unified is not None:
            # Ghi vào PostgreSQL (nếu dùng)
            # postgres_config = get_postgres_config()
            # write_to_postgres(df_unified, postgres_config)
        
            # Lưu vào MinIO
            save_processed_data_to_minIO(spark, bucket_name, df_unified)
            
            # Lưu vào thư mục processed_data ở local
            prefix = "processed/restaurants_unified/"
            local_dir = "processed_data/"
            save_processed_data_to_local(client, bucket_name, prefix, local_dir)
    
    spark.stop()

if __name__ == "__main__":
    main()
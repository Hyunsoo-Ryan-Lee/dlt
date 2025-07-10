import dlt, requests
import pandas as pd

def get_customer_data(url: str) -> pd.DataFrame:
    """API로부터 고객 데이터를 가져오는 함수"""
    response = requests.get(url)
    df = pd.json_normalize(response.json())
    return df

def load_to_bigquery(data: pd.DataFrame, **kwargs):
    """데이터를 BigQuery에 적재하는 함수"""
    pipeline = dlt.pipeline(
        pipeline_name=kwargs["pipeline_name"],
        destination="bigquery", 
        dataset_name=kwargs["dataset_name"]
    )
    return pipeline

if __name__ == "__main__":
    # API URL 설정
    API_URL = "https://jaffle-shop.scalevector.ai/api/v1/customers"

    # 데이터 가져오기
    customer_df = get_customer_data(API_URL)

    # BigQuery에 데이터 적재
    pipeline = load_to_bigquery(
        data=customer_df,
        pipeline_name="api_bq_pipeline",
        dataset_name="bq_dlt"
    )

    result = pipeline.run(data=customer_df, table_name="customers")

    print("BigQuery 파이프라인 실행 결과:", result)
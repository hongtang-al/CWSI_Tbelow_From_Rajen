import io

import boto3
from tqdm import tqdm
import pandas as pd
import numpy as np


def get_roc(df, col_of_interest, periods_back=4, verbose=True, device_present=True):

    def calc_slope(y):
        x = pd.Series(range(len(y)))
        # handle NAS
        idx = np.isfinite(y.to_numpy())
        slope = np.polyfit(x[idx], y[idx], 1)[0]
        return slope
    min_periods = 2
    if device_present:
        if df["device"].isna().all():
            df[f"{col_of_interest}_ROC"] = np.nan
            return df
        new_dfs = []
        for device, device_df in (
            tqdm(df.groupby("device")) if verbose else df.groupby("device")
        ):
            device_df.sort_values("time", inplace=True)
            device_df[f"{col_of_interest}_ROC"] = (
                device_df[col_of_interest]
                .rolling(periods_back, min_periods=min_periods)
                .apply(calc_slope)
            )
            new_dfs.append(device_df)

        if len(new_dfs) > 0:
            return pd.concat(new_dfs)
        else:
            df[f"{col_of_interest}_ROC"] = np.nan
            return df
    else:
        df.sort_values("time", inplace=True)
        df[f"{col_of_interest}_ROC"] = (
            df[col_of_interest]
            .rolling(periods_back, min_periods=min_periods)
            .apply(calc_slope)
        )
        return df


def df_to_s3(df, key, bucket, verbose=True, format="csv"):
    if format == "csv":
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
    elif format == "parquet":
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
    else:
        raise Exception(f"format '{format}' not recognized")
    # write stream to S3
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    if verbose:
        print(f"Uploaded file to s3://{bucket}/{key}")


def df_from_s3(key, bucket, format="csv", **kwargs):
    """read csv from S3 as pandas df
    Arguments:
        key - key of file on S3
        bucket - bucket of file on S3
        **kwargs - additional keyword arguments to pass pd.read_ methods
    Returns:
        df - pandas df
    """
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]
    if format == "csv":
        csv_string = body.read().decode("utf-8")
        df = pd.read_csv(io.StringIO(csv_string), **kwargs)
    elif format == "parquet":
        bytes_obj = body.read()
        df = pd.read_parquet(io.BytesIO(bytes_obj), **kwargs)
    else:
        raise Exception(f"format '{format}' not recognized")

    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'], errors='coerce', utc=True)
    if 'ref_time' in df.columns:
        df['ref_time'] = pd.to_datetime(df['ref_time'], errors='coerce', utc=True)
    return df


def list_keys(prefix, bucket):
    """return list of files with specific S3 prefix"""
    bucket_resource = boto3.resource("s3").Bucket(bucket)
    key_list = [file.key for file in bucket_resource.objects.filter(Prefix=prefix)]
    return key_list
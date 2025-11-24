import json
import time
from pathlib import Path

import polars as pl
import requests as reqs
from rich import print

from .constants import *


class APIException(Exception): ...


def read_csv(fname: Path) -> pl.DataFrame:
    """Read a csv in the CRM format.

    Note:
        This converts field names to those needed
        for the Surfe API.

    Args:
        fname (Path): input csv

    Returns:
        pl.DataFrame: dataframe
    """
    df = pl.read_csv(fname)

    # rename columns for the surfe API
    df = df.select(
        pl.col("CRM ID").alias("externalID").cast(pl.String),
        pl.col("First Name").alias("firstName"),
        pl.col("Last Name").alias("lastName"),
        pl.col("Email").alias("email"),
        pl.col("LinkedIn URL").alias("linkedInUrl"),
    )
    return df


def post_enrichment(df: pl.DataFrame) -> reqs.Response:
    """Post a dtaframe for Surfe enrichment.

    Args:
        df (pl.DataFrame): input dataframe

    Returns:
        reqs.Response: surfe API response
    """
    json = {
        "include": {"email": True, "linkedInUrl": False, "mobile": False},
        "people": df.to_dicts(),
    }

    return reqs.post(API_URL, json=json, headers=API_HEADERS)


def get_enrichment(results_url: str) -> reqs.Response:
    """Get enrichment results.

    Args:
        results_url (str): url for the results

    Returns:
        reqs.Response: results response
    """
    return reqs.get(results_url, headers=API_HEADERS)


def enrich(fname: Path, freq: int = 2) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Submit a csv for Surfe enrichment.

    Args:
        fname (Path): input csv from CRM
        freq (int, optional): how frequently to check for the results (seconds).
            Defaults to 2.

    Raises:
        APIException: problem submitting csv
        APIException: problem getting results

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: input dataframe, api dataframe
    """

    df1 = read_csv(fname)

    start_resp = post_enrichment(df1)

    if not start_resp.ok:
        print(start_resp)
        msg = f"Encountered a problem submitting csv to surfe:\n{start_resp.text}"
        raise APIException(msg)

    start_resp_json = json.loads(start_resp.text)

    print(f"Successfully submitted {fname}!")
    print(start_resp_json)

    # TODO: add max iterations
    while True:
        results_resp = get_enrichment(start_resp_json["enrichmentCallbackURL"])

        if not results_resp.status_code == 200:
            msg = (
                f"Encountered a problem getting results from surfe:\n{start_resp.text}"
            )
            raise APIException(msg)

        results_resp_json = json.loads(results_resp.text)

        print(f"Percent completed: {results_resp_json['percentCompleted']}%")

        if results_resp_json["status"] == "COMPLETED":
            # got completed results
            break

        time.sleep(freq)  # seconds

    df2 = pl.from_dicts(results_resp_json["people"])

    return df1, df2


def diff(df_crm: pl.DataFrame, df_surfe: pl.DataFrame) -> pl.DataFrame:
    """Find the differences between a CRM dataframe and a Surfe dataframe.

    Args:
        df_crm (pl.DataFrame): CRM dataframe
        df_surfe (pl.DataFrame): Surfe dataframe

    Returns:
        pl.DataFrame: joined dataframe with columns indicating differences
    """
    df_flat_join = df_crm.join(df_surfe, on="externalID", suffix="_surfe")

    df_email_explode = (
        df_flat_join.select(
            pl.exclude("emails", "email"),
            pl.col("email").alias("email_df1"),
            pl.col("emails").explode().struct.unnest(),
        )
        .select(pl.exclude("email"), pl.col("email").alias("email_surfe"))
        .filter(pl.col("validationStatus").is_in(["VALID", "CATCH_ALL"]))
    )

    df_emails = df_email_explode.group_by("externalID").agg(
        pl.col("email_surfe"), pl.col("validationStatus")
    )

    df_join_emails = df_flat_join.join(df_emails, on="externalID", how="left")

    df_join_emails = df_join_emails.select(
        pl.all(),
        pl.col("email_surfe")
        .list.contains(pl.col("email"))
        .alias("crm_email_in_surfe_results"),
    )

    df_diff = df_join_emails.with_columns(
        has_diff=pl.any_horizontal(
            *list(
                pl.col(x).ne_missing(pl.col(f"{x}_surfe"))
                for x in df_crm.columns
                if x not in ["externalID", "email", "linkedInUrl"]
            ),
            pl.col("linkedInUrl").str.extract("/in/(\w+)/{0,1}")
            != pl.col("linkedInUrl_surfe").str.extract("/in/(\w+)/{0,1}"),
        )
    )

    df_out = df_diff.select(
        pl.exclude("email_surfe", "validationStatus", "emails", "mobilePhones"),
        pl.col("email_surfe").list.join(";").alias("emails_surfe"),
        pl.col("validationStatus").list.join(";").alias("emails_surfeValidationStatus"),
    )

    return df_out


def clean_df_columns(out_df: pl.DataFrame) -> pl.DataFrame:
    """Clean output dataframe columns.

    Args:
        out_df (pl.DataFrame): df to clean

    Returns:
        pl.DataFrame: cleaned df
    """

    return out_df.select(OUTPUT_COLUMNS_SAFE)

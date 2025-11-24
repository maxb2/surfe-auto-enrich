from pathlib import Path
from typing import Annotated, Optional

import polars as pl
import typer

from .operations import clean_df_columns, diff, enrich

app = typer.Typer()


@app.command()
def submit_enrichment(
    input: Annotated[Path, typer.Argument(help="input csv")],
    output: Annotated[Optional[Path], typer.Argument(help="output csv")] = None,
    diffs_only: Annotated[
        Optional[bool], typer.Option(help="whether to output only differences")
    ] = False,
    freq: Annotated[
        Optional[int],
        typer.Option(help="how frequently to check for the results (seconds)"),
    ] = 10,
    clean_columns: Annotated[
        Optional[bool], typer.Option(help="whether to clean the output CSV columns")
    ] = True,
):
    """Surfe Enrichment.

    The input csv needs to have these field names:\n
    CRM ID,First Name,Last Name,Email,LinkedIn URL\n
    They will get mapped to the associated names for the Surfe API.
    """

    if output is None:
        output = input.with_suffix(".out.csv")

    df1, df2 = enrich(input, freq=freq)

    diff_df = diff(df1, df2)

    if diffs_only:
        diff_df = diff_df.filter(pl.col("has_diff") & pl.col("old_email_in_results"))

    if clean_columns:
        diff_df = clean_df_columns(diff_df)

    diff_df.write_csv(output)


if __name__ == "__main__":
    app()

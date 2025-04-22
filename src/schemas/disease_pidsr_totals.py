import polars as pl

DiseasePidsrTotals = pl.Struct(
    [
        pl.Field("uuid", pl.String()),
        pl.Field("freq", pl.Enum(["W"])),
        pl.Field("date", pl.Date()),
        pl.Field("source_name", pl.String()),
        pl.Field("source_filename", pl.String()),
        pl.Field("adm3_pcode", pl.String()),
        pl.Field("disease_icd10_code", pl.String()),
        pl.Field("disease_common_name", pl.String()),
        pl.Field("case_total", pl.Int16()),
    ]
)

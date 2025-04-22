import polars as pl

Location = pl.Struct(
    [
        pl.Field("adm1_en", pl.String()),
        pl.Field("adm1_pcode", pl.String()),
        pl.Field("adm2_en", pl.String()),
        pl.Field("adm2_pcode", pl.String()),
        pl.Field("adm3_en", pl.String()),
        pl.Field("adm3_pcode", pl.String()),
        pl.Field("adm4_en", pl.String()),
        pl.Field("adm4_pcode", pl.String()),
        pl.Field("brgy_total_area", pl.Float32()),
    ]
)

import polars as pl

ClimateAtmosphere = pl.Struct(
    [
        pl.Field("uuid", pl.String()),
        pl.Field("adm4_pcode", pl.String()),
        pl.Field("date", pl.Date()),
        pl.Field("freq", pl.Enum(["D"])),
        pl.Field("tave", pl.Float32()),
        pl.Field("tmin", pl.Float32()),
        pl.Field("tmax", pl.Float32()),
        pl.Field("heat_index", pl.Float32()),
        pl.Field("pr", pl.Float32()),
        pl.Field("wind_speed", pl.Float32()),
        pl.Field("rh", pl.Float32()),
        pl.Field("solar_rad", pl.Float32()),
        pl.Field("uv_rad", pl.Float32()),
    ]
)

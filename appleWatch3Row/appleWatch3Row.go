package appleWatch3Row

type AppleWatch3Row struct {
	Ts uint64  `parquet:"name=ts, type=UINT_64"`
	Rx float64 `parquet:"name=rx, type=DOUBLE"`
	Ry float64 `parquet:"name=ry, type=DOUBLE"`
	Rz float64 `parquet:"name=rz, type=DOUBLE"`
	Rl float64 `parquet:"name=rl, type=DOUBLE"`
	Pt float64 `parquet:"name=pt, type=DOUBLE"`
	Yw float64 `parquet:"name=yw, type=DOUBLE"`
	Ax float64 `parquet:"name=ax, type=DOUBLE"`
	Ay float64 `parquet:"name=ay, type=DOUBLE"`
	Az float64 `parquet:"name=az, type=DOUBLE"`
	Hr float64 `parquet:"name=hr, type=DOUBLE"`
}

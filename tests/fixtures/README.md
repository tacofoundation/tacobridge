# Test Fixtures

Small TACO datasets for testing export, conversion, and concat operations.

## Regenerate

```bash
python regenerate.py
```

## Structure

```
fixtures/
├── zip/
│   ├── flat_a/flat_a.tacozip      # 10 FILEs, region=west
│   ├── flat_b/flat_b.tacozip      # 10 FILEs, region=east
│   ├── nested_a/nested_a.tacozip  # 5 FOLDERs × 3 children
│   ├── nested_b/nested_b.tacozip  # 5 FOLDERs × 3 children
│   └── deep/deep.tacozip          # 3 levels: 3×2×2
└── folder/
    ├── flat_a/
    └── nested_a/
```

## Datasets

| Dataset | Levels | Samples | Schema |
|---------|--------|---------|--------|
| flat_a | 1 | 10 FILEs | cloud_cover, location, region=west |
| flat_b | 1 | 10 FILEs | cloud_cover, location, region=east |
| nested_a | 2 | 5 FOLDERs, 15 FILEs | cloud_cover, location, band |
| nested_b | 2 | 5 FOLDERs, 15 FILEs | cloud_cover, location, band |
| deep | 3 | 3 regions × 2 sensors × 2 bands | wavelength, resolution |

## Test Coverage

| Feature | Fixtures |
|---------|----------|
| `export` with SQL filter | flat_a |
| `export` with RANDOM() | flat_a |
| `export` with bbox | flat_a |
| `concat` + `export` | flat_a + flat_b |
| `zip2folder` | any .tacozip |
| `folder2zip` | folder/flat_a |
| Nested recursion | nested_a |
| Deep recursion | deep |
| Composite keying | flat_a + flat_b (concat) |

## Filtering Examples

```python
import tacoreader

ds = tacoreader.load("fixtures/zip/flat_a/flat_a.tacozip")

# SQL filter
ds.sql("SELECT * FROM data WHERE cloud_cover < 50")  # 5 samples

# Bbox filter (Madrid)
ds.filter_bbox(-3.9, 40.2, -3.5, 40.5)  # 2 samples

# Concat
ds_a = tacoreader.load("fixtures/zip/flat_a/flat_a.tacozip")
ds_b = tacoreader.load("fixtures/zip/flat_b/flat_b.tacozip")
ds_concat = tacoreader.concat([ds_a, ds_b])  # 20 samples
```
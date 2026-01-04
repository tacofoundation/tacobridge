"""
Test fixtures for tacobridge.

Generates small TACO datasets for testing:
- export (filtered datasets)
- zip2folder / folder2zip conversion
- concatenated dataset export

Fixtures use bytes as content (no GeoTIFFs needed).

Output:
    tests/fixtures/
    ├── zip/
    │   ├── flat_a/flat_a.tacozip      # 10 FILEs, region=west
    │   ├── flat_b/flat_b.tacozip      # 10 FILEs, region=east (concat)
    │   ├── nested_a/nested_a.tacozip  # 5 FOLDERs x 3 children
    │   └── nested_b/nested_b.tacozip  # 5 FOLDERs x 3 children (concat)
    └── folder/
        ├── flat_a/                     # Same as zip/flat_a
        └── nested_a/                   # Same as zip/nested_a

Usage:
    python regenerate.py
"""

import pathlib
import shutil
import struct
from datetime import datetime, timedelta

import tacotoolbox
from tacotoolbox.datamodel import Sample, Tortilla, Taco

FIXTURES_DIR = pathlib.Path(__file__).parent
ZIP_DIR = FIXTURES_DIR / "zip"
FOLDER_DIR = FIXTURES_DIR / "folder"

BASE_DATE = datetime(2024, 1, 1)

COLLECTION_BASE = {
    "dataset_version": "1.0.0",
    "description": "tacobridge test fixture",
    "licenses": ["CC-BY-4.0"],
    "providers": [{"name": "Test", "roles": ["producer"]}],
    "tasks": ["classification"],
}

# Bounding boxes for spatial filtering
BBOXES = {
    "madrid": (-3.9, 40.2, -3.5, 40.5),
    "barcelona": (2.0, 41.3, 2.3, 41.5),
    "valencia": (-0.5, 39.3, -0.2, 39.6),
    "sevilla": (-6.1, 37.3, -5.8, 37.5),
    "bilbao": (-3.0, 43.2, -2.8, 43.4),
    "paris": (2.2, 48.7, 2.5, 49.0),
    "berlin": (13.2, 52.3, 13.6, 52.6),
    "rome": (12.3, 41.7, 12.6, 42.0),
    "london": (-0.3, 51.4, 0.0, 51.6),
    "amsterdam": (4.7, 52.3, 5.0, 52.5),
}


def _polygon_wkb(minx: float, miny: float, maxx: float, maxy: float) -> bytes:
    """Create WKB polygon from bbox."""
    wkb = struct.pack("<bII", 1, 3, 1)  # LE, Polygon, 1 ring
    wkb += struct.pack("<I", 5)  # 5 points (closed)
    for x, y in [(minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy), (minx, miny)]:
        wkb += struct.pack("<dd", x, y)
    return wkb


def _point_wkb(lon: float, lat: float) -> bytes:
    """Create WKB point."""
    return struct.pack("<bIdd", 1, 1, lon, lat)


def _centroid(bbox: tuple[float, float, float, float]) -> tuple[float, float]:
    """Get centroid of bbox."""
    return ((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2)


def _date(days: int) -> datetime:
    """Get date offset from BASE_DATE."""
    return BASE_DATE + timedelta(days=days)


def create_flat_a(output: pathlib.Path) -> pathlib.Path:
    """
    10 FILEs with region=west, varied cloud_cover and dates.
    
    IDs: sample_000 to sample_009
    Locations: Spanish cities
    cloud_cover: 0, 10, 20, ..., 90
    dates: 2024-01-01 + i*7 days
    """
    locations = ["madrid", "barcelona", "valencia", "sevilla", "bilbao"] * 2
    
    samples = []
    for i in range(10):
        loc = locations[i]
        bbox = BBOXES[loc]
        cx, cy = _centroid(bbox)
        
        sample = Sample(id=f"sample_{i:03d}", path=f"content_{i:03d}".encode())
        sample.extend_with({
            "istac:geometry": _polygon_wkb(*bbox),
            "istac:centroid": _point_wkb(cx, cy),
            "istac:time_start": _date(i * 7),
            "cloud_cover": float(i * 10),
            "location": loc,
            "region": "west",
            "quality": i % 3,  # 0, 1, 2 cycling
        })
        samples.append(sample)
    
    taco = Taco(
        id="flat_a",
        tortilla=Tortilla(samples),
        **COLLECTION_BASE,
    )
    return tacotoolbox.create(taco, output)[0]


def create_flat_b(output: pathlib.Path) -> pathlib.Path:
    """
    10 FILEs with region=east, same schema as flat_a for concat.
    
    IDs: sample_000 to sample_009 (same IDs, different content)
    Locations: European cities
    cloud_cover: 5, 15, 25, ..., 95
    dates: 2024-04-01 + i*7 days
    """
    locations = ["paris", "berlin", "rome", "london", "amsterdam"] * 2
    
    samples = []
    for i in range(10):
        loc = locations[i]
        bbox = BBOXES[loc]
        cx, cy = _centroid(bbox)
        
        sample = Sample(id=f"sample_{i:03d}", path=f"content_b_{i:03d}".encode())
        sample.extend_with({
            "istac:geometry": _polygon_wkb(*bbox),
            "istac:centroid": _point_wkb(cx, cy),
            "istac:time_start": _date(90 + i * 7),  # Start April
            "cloud_cover": float(i * 10 + 5),
            "location": loc,
            "region": "east",
            "quality": (i + 1) % 3,
        })
        samples.append(sample)
    
    taco = Taco(
        id="flat_b",
        tortilla=Tortilla(samples),
        **COLLECTION_BASE,
    )
    return tacotoolbox.create(taco, output)[0]


def create_nested_a(output: pathlib.Path) -> pathlib.Path:
    """
    5 FOLDERs x 3 children each = 15 leaf files.
    
    FOLDERs: folder_000 to folder_004 (Spanish cities)
    Children: item_0, item_1, item_2 (same IDs per RSUT)
    """
    folder_locs = ["madrid", "barcelona", "valencia", "sevilla", "bilbao"]
    
    folders = []
    for i, loc in enumerate(folder_locs):
        bbox = BBOXES[loc]
        cx, cy = _centroid(bbox)
        
        children = []
        for j in range(3):
            child = Sample(id=f"item_{j}", path=f"data_{i}_{j}".encode())
            child.extend_with({
                "band": ["R", "G", "B"][j],
                "value": float(i * 10 + j),
            })
            children.append(child)
        
        folder = Sample(id=f"folder_{i:03d}", path=Tortilla(children))
        folder.extend_with({
            "istac:geometry": _polygon_wkb(*bbox),
            "istac:centroid": _point_wkb(cx, cy),
            "istac:time_start": _date(i * 14),
            "cloud_cover": float(i * 15),
            "location": loc,
            "region": "west",
        })
        folders.append(folder)
    
    taco = Taco(
        id="nested_a",
        tortilla=Tortilla(folders),
        **COLLECTION_BASE,
    )
    return tacotoolbox.create(taco, output)[0]


def create_nested_b(output: pathlib.Path) -> pathlib.Path:
    """
    5 FOLDERs x 3 children each, same schema as nested_a for concat.
    
    FOLDERs: folder_000 to folder_004 (European cities)
    Children: item_0, item_1, item_2
    """
    folder_locs = ["paris", "berlin", "rome", "london", "amsterdam"]
    
    folders = []
    for i, loc in enumerate(folder_locs):
        bbox = BBOXES[loc]
        cx, cy = _centroid(bbox)
        
        children = []
        for j in range(3):
            child = Sample(id=f"item_{j}", path=f"data_b_{i}_{j}".encode())
            child.extend_with({
                "band": ["R", "G", "B"][j],
                "value": float(50 + i * 10 + j),
            })
            children.append(child)
        
        folder = Sample(id=f"folder_{i:03d}", path=Tortilla(children))
        folder.extend_with({
            "istac:geometry": _polygon_wkb(*bbox),
            "istac:centroid": _point_wkb(cx, cy),
            "istac:time_start": _date(90 + i * 14),
            "cloud_cover": float(i * 15 + 10),
            "location": loc,
            "region": "east",
        })
        folders.append(folder)
    
    taco = Taco(
        id="nested_b",
        tortilla=Tortilla(folders),
        **COLLECTION_BASE,
    )
    return tacotoolbox.create(taco, output)[0]


def create_deep(output: pathlib.Path) -> pathlib.Path:
    """
    3 levels: 3 regions x 2 sensors x 2 bands = 12 leaf files.
    
    L0: region_0, region_1, region_2 (FOLDERs with geometry)
    L1: sensor_0, sensor_1 (FOLDERs)
    L2: band_0, band_1 (FILEs)
    """
    region_locs = ["madrid", "paris", "berlin"]
    
    regions = []
    for i, loc in enumerate(region_locs):
        bbox = BBOXES[loc]
        cx, cy = _centroid(bbox)
        
        sensors = []
        for s in range(2):
            bands = []
            for b in range(2):
                band = Sample(id=f"band_{b}", path=f"pixels_{i}_{s}_{b}".encode())
                band.extend_with({
                    "wavelength": [665, 560][b],
                    "resolution": [10, 20][s],
                })
                bands.append(band)
            
            sensor = Sample(id=f"sensor_{s}", path=Tortilla(bands))
            sensor.extend_with({"sensor_type": ["MSI", "SAR"][s]})
            sensors.append(sensor)
        
        region = Sample(id=f"region_{i}", path=Tortilla(sensors))
        region.extend_with({
            "istac:geometry": _polygon_wkb(*bbox),
            "istac:centroid": _point_wkb(cx, cy),
            "istac:time_start": _date(i * 30),
            "location": loc,
        })
        regions.append(region)
    
    taco = Taco(
        id="deep",
        tortilla=Tortilla(regions),
        **COLLECTION_BASE,
    )
    return tacotoolbox.create(taco, output)[0]


def main():
    print("Regenerating tacobridge fixtures...\n")
    
    # Clean
    for d in [ZIP_DIR, FOLDER_DIR]:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)
    
    # ZIP formats
    print("Creating ZIP fixtures:")
    
    p = create_flat_a(ZIP_DIR / "flat_a" / "flat_a.tacozip")
    print(f"  ✓ {p.relative_to(FIXTURES_DIR)}")
    
    p = create_flat_b(ZIP_DIR / "flat_b" / "flat_b.tacozip")
    print(f"  ✓ {p.relative_to(FIXTURES_DIR)}")
    
    p = create_nested_a(ZIP_DIR / "nested_a" / "nested_a.tacozip")
    print(f"  ✓ {p.relative_to(FIXTURES_DIR)}")
    
    p = create_nested_b(ZIP_DIR / "nested_b" / "nested_b.tacozip")
    print(f"  ✓ {p.relative_to(FIXTURES_DIR)}")
    
    p = create_deep(ZIP_DIR / "deep" / "deep.tacozip")
    print(f"  ✓ {p.relative_to(FIXTURES_DIR)}")
    
    # FOLDER formats
    print("\nCreating FOLDER fixtures:")
    
    p = create_flat_a(FOLDER_DIR / "flat_a")
    print(f"  ✓ {p.relative_to(FIXTURES_DIR)}")
    
    p = create_nested_a(FOLDER_DIR / "nested_a")
    print(f"  ✓ {p.relative_to(FIXTURES_DIR)}")
    
    print("\nDone!")
    print(f"\nFixtures: {FIXTURES_DIR}")


if __name__ == "__main__":
    main()
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    data: Path
    raw: Path
    processed: Path
    cache: Path
    external: Path

def build_paths(root_dir: Path) -> ProjectPaths:
    data_dir = root_dir / "data"

    return ProjectPaths(
        root=root_dir,
        data=data_dir,
        raw=data_dir / "raw",
        processed=data_dir / "processed",
        cache=data_dir / "cache",
        external=data_dir / "external",
    )

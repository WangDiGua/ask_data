from pathlib import Path


def test_package_init_exists() -> None:
    assert Path("src/ndea/__init__.py").exists()

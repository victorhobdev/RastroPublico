def test_package_exposes_version() -> None:
    import rastro_publico

    assert rastro_publico.__version__ == "0.1.0"

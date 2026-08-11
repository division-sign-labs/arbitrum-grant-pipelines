"""Offline test suite for the Arbitrum grant pipelines.

Everything here runs without a network, a Dune key, a Neynar key or a Neo4j
instance. HTTP is served by `requests_mock` against recorded payload shapes in
`tests/fixtures/`; the filesystem layout (DATA_DIR / STATE_DIR / SEEDS_DIR) is
redirected into a tmp_path by an autouse fixture in `conftest.py`.
"""

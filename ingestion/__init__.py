"""CSV run -> Neo4j.

One module per data type (`ingestion.ingest_<data_type>`), each of which reads a
completed run under data/<data_type>/<run_ts>/ and MERGEs it into the graph.
`ingestion.base` holds everything they share, the Cypher itself lives in
`cypher.<data_type>`, and `ingestion.constraints` applies the schema constraints
that make those MERGEs safe.

Nothing here queries an upstream API or spends credits: ingestion is a pure
function of a run directory, so it can be re-run at will.
"""

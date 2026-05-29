"""alation-rdf-sync: scheduled adapter for the Alation -> Stardog bridge.
Stages run in order each tick: sync -> extract_concepts -> suggest -> writeback.
See ../../Alation-Stardog-Design-v1.0.md for the authoritative contract."""
__version__ = "1.0.0"

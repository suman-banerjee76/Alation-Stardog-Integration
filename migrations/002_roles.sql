-- Roles + grants. Run as superuser. Invariant: adapter is the only writer.
-- CREATE ROLE alation_writer LOGIN PASSWORD :'writer_pw';
-- CREATE ROLE stardog_reader LOGIN PASSWORD :'reader_pw';
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO alation_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO stardog_reader;
-- writeback_audit is append-only: no UPDATE/DELETE for anyone.
REVOKE UPDATE, DELETE ON writeback_audit FROM alation_writer, stardog_reader, PUBLIC;

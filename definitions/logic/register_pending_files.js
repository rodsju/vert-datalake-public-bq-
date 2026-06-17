// definitions/logic/register_pending_files.js

const { tables } = ingestor_config;

tables
  .filter((t) => (t.mode || "snapshot") === "file_incremental")
  .forEach((t) => {
    const name = t.name;

    operate(`register_pending_files_${name}`)
      .schema("logic")
      .hasOutput(false)
      .tags(["register_pending_files", `register_pending_files_${name}`])
      .dependencies(["file_ingestion", `${name}_ext`])
      .queries((ctx) => `
INSERT INTO \`${ctx.database()}.logic.file_ingestion\` (
  source_table,
  gcs_uri,
  file_name,
  file_prefix,
  status,
  is_active,
  created_at
)
SELECT DISTINCT
  '${name}' AS source_table,
  _FILE_NAME AS gcs_uri,
  REGEXP_EXTRACT(_FILE_NAME, r'([^/]+)$') AS file_name,
  REGEXP_EXTRACT(_FILE_NAME, r'([^/]+)__[0-9]{8}T[0-9]{6}Z\\.parquet$') AS file_prefix,
  'PENDING' AS status,
  FALSE AS is_active,
  CURRENT_TIMESTAMP() AS created_at
FROM \`${ctx.database()}.bronze_ext.${name}_full\`
WHERE _FILE_NAME NOT IN (
  SELECT gcs_uri
  FROM \`${ctx.database()}.logic.file_ingestion\`
  WHERE source_table = '${name}'
)
`);
  });
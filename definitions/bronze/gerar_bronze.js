// definitions/bronze/gerar_bronze.js

const { tables } = ingestor_config;

tables.forEach((t) => {
  const name = t.name;
  const frequency = t.frequency || "never";
  const mode = t.mode || "snapshot";

  const exceptColumns = t.exceptColumns || ["raw_data"];

  const selectStar = exceptColumns.length
    ? `* EXCEPT(${exceptColumns.map((c) => `\`${c}\``).join(", ")})`
    : "*";

  const extSelectStar = exceptColumns.length
    ? `ext.* EXCEPT(${exceptColumns.map((c) => `\`${c}\``).join(", ")})`
    : "ext.*";

  const tablePartitionClause = t.partitionBy
    ? `\nPARTITION BY ${t.partitionBy}`
    : "";

  const tableClusterClause = t.clusterBy && t.clusterBy.length
    ? `\nCLUSTER BY ${t.clusterBy.map((c) => `\`${c}\``).join(", ")}`
    : "";

  const bigquery = {};

  if (t.partitionBy) {
    bigquery.partitionBy = t.partitionBy;
  }

  if (t.clusterBy && t.clusterBy.length) {
    bigquery.clusterBy = t.clusterBy;
  }

  /**
   * MODO 1: snapshot - 
   *
   * Recria a bronze inteira sempre.
   * Bom para tabelas pequenas ou snapshots oficiais.
   */
  if (mode === "snapshot") {
    publish(name, {
      schema: "bronze",
      type: "table",
      dependencies: [`${name}_ext`],
      tags: ["bronze", `bronze_${name}`, `frequency_${frequency}`],
      ...(Object.keys(bigquery).length ? { bigquery } : {}),
    }).query((ctx) => `
      SELECT
        ${selectStar},
        REGEXP_EXTRACT(_FILE_NAME, r'([^/]+)__[0-9]{8}T[0-9]{6}Z\\.parquet$') AS file_prefix,
        _FILE_NAME AS _source_gcs_uri,
        REGEXP_EXTRACT(_FILE_NAME, r'([^/]+)$') AS _source_file,
        CURRENT_TIMESTAMP() AS _processed_at
      FROM \`${ctx.database()}.bronze_ext.${t.name}_full\`
    `);

    return;
  }

  /**
   * MODO 2: file_incremental
   *
   * Processa apenas arquivos PENDING em logic.file_ingestion.
   * Para tabelas normais: substitui por file_prefix.
   * Para tabelas com dedup + replaceByKeys: substitui por chave lógica.
   */
  const keys = t.keys || [];
  const keyPartitionBy = keys.map((k) => `\`${k}\``).join(", ");
  const orderBy = t.orderBy || "_processed_at DESC";

  const replaceByKeys = !!t.replaceByKeys && keys.length > 0;

  const keyJoinCondition = keys
    .map((k) => `bronze.\`${k}\` = final_data.\`${k}\``)
    .join(" AND ");

  const stagedFinalSql = (() => {
    if (!t.dedup) {
      return `
        SELECT *
        FROM staged_data
      `;
    }

    return `
      SELECT *
      FROM staged_data
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ${keyPartitionBy}
        ORDER BY ${orderBy}
      ) = 1
    `;
  })();

  const deleteSql = (() => {
    if (replaceByKeys) {
      return `
        DELETE FROM \`${"${ctx.database()}"}.bronze.${name}\` AS bronze
        WHERE EXISTS (
          SELECT 1
          FROM final_data
          WHERE ${keyJoinCondition}
        );
      `;
    }

    return `
      DELETE FROM \`${"${ctx.database()}"}.bronze.${name}\`
      WHERE file_prefix IN (
        SELECT DISTINCT file_prefix
        FROM final_data
      );
    `;
  })();

  operate(name)
    .hasOutput(true)
    .schema("bronze")
    .tags(["bronze", `bronze_${name}`], `frequency_${frequency}`)
    .dependencies([`register_pending_files_${name}`])
    .queries((ctx) => {
      const renderedDeleteSql = replaceByKeys
        ? `
    DELETE FROM \`${ctx.database()}.bronze.${name}\` AS bronze
    WHERE EXISTS (
      SELECT 1
      FROM final_data
      WHERE ${keyJoinCondition}
    );
        `
        : `
    DELETE FROM \`${ctx.database()}.bronze.${name}\`
    WHERE file_prefix IN (
      SELECT DISTINCT file_prefix
      FROM final_data
    );
        `;

      return `
DECLARE target_exists BOOL;

SET target_exists = (
  SELECT COUNT(*) > 0
  FROM \`${ctx.database()}.bronze.INFORMATION_SCHEMA.TABLES\`
  WHERE table_name = '${name}'
);

CREATE TEMP TABLE pending_files AS
SELECT DISTINCT
  gcs_uri,
  file_name,
  file_prefix
FROM \`${ctx.database()}.logic.file_ingestion\`
WHERE source_table = '${name}'
  AND status = 'PENDING';

IF (SELECT COUNT(*) FROM pending_files) > 0 THEN

  CREATE TEMP TABLE staged_data AS
  SELECT
    ${extSelectStar},
    pf.file_prefix AS file_prefix,
    _FILE_NAME AS _source_gcs_uri,
    REGEXP_EXTRACT(_FILE_NAME, r'([^/]+)$') AS _source_file,
    CURRENT_TIMESTAMP() AS _processed_at
  FROM \`${ctx.database()}.bronze_ext.${name}_full\` ext
  JOIN pending_files pf
    ON ext._FILE_NAME = pf.gcs_uri;

  CREATE TEMP TABLE final_data AS
  ${stagedFinalSql};

  IF NOT target_exists THEN

    EXECUTE IMMEDIATE """
      CREATE TABLE \`${ctx.database()}.bronze.${name}\`
      ${tablePartitionClause}
      ${tableClusterClause}
      AS
      SELECT *
      FROM final_data
    """;

  ELSE

${renderedDeleteSql}

    INSERT INTO \`${ctx.database()}.bronze.${name}\`
    SELECT *
    FROM final_data;

  END IF;

  UPDATE \`${ctx.database()}.logic.file_ingestion\`
  SET
    status = 'REPLACED',
    is_active = FALSE,
    replaced_at = CURRENT_TIMESTAMP()
  WHERE source_table = '${name}'
    AND status = 'PROCESSED'
    AND is_active = TRUE
    AND file_prefix IN (
      SELECT DISTINCT file_prefix
      FROM final_data
    );

  UPDATE \`${ctx.database()}.logic.file_ingestion\` fi
  SET
    status = 'PROCESSED',
    is_active = TRUE,
    processed_at = CURRENT_TIMESTAMP(),
    row_count = fd.row_count
  FROM (
    SELECT
      _source_gcs_uri,
      COUNT(*) AS row_count
    FROM final_data
    GROUP BY _source_gcs_uri
  ) fd
  WHERE fi.source_table = '${name}'
    AND fi.status = 'PENDING'
    AND fi.gcs_uri = fd._source_gcs_uri;

END IF;
`;
    });
});
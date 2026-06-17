const { BUCKET, PREFIX, tables } = ingestor_config;

tables.forEach((t) => {
  const source = t.source || `${PREFIX}/${t.name}`;

  operate(`${t.name}_ext`)
    .tags(["setup_bronze_ext", `ingestor_${t.name}_ext`, `bronze_ext_${t.name}`])
    .dependencies(["create_datasets"])
    .queries(
      (ctx) => `
      CREATE OR REPLACE EXTERNAL TABLE \`${ctx.database()}.bronze_ext.${t.name}_full\`
      OPTIONS (
        format = 'PARQUET',
        uris = ['${BUCKET}/${source}/*.parquet'],
        decimal_target_types = ['BIGNUMERIC']
      )`
    );

});
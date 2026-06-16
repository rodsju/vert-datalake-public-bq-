const BUCKET = "gs://vert-dev-datalake-public-raw";
const PREFIX = "ingestor";

const tables = [
    {
        name: "accounts_user"
    },
    {
        name: "auth_permission"
    },
    {
        name: "unit_price_unitprice",
        partitionBy: "PARTITION BY DATE_TRUNC(data_referencia, YEAR)",
        clusterBy: ["serie_id", "lastro_id", "curve_type"]
    }
]

module.exports = {
    BUCKET,
    PREFIX,
    tables
};
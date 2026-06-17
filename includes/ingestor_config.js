const BUCKET = "gs://vert-dev-datalake-public-raw";
const PREFIX = "ingestor";

const tables = [
        { name: "cvm_fidc_reports" },
        { name: "cvm_cri_reports" },
        { name: "cvm_cra_reports" },
        { name: "cvm_fi_holdings" },
        { name: "cvm_fi_cadastral" },
        { name: "cvm_debenture_offers" },
        { name: "cvm_sre_offerings" },
        { name: "cvm_dfin_docs" },
        { name: "cvm_fi_docs_eventual" },
        { name: "b3_negociacoes" },

        {
            name: "fnet_documentos",
            mode: "file_incremental",
            dedup: true,
            keys: ["id_externo"],
            orderBy: "data_entrega DESC NULLS LAST",
            replaceByKeys: true,
        },
]

module.exports = {
    BUCKET,
    PREFIX,
    tables
};
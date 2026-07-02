const BUCKET = "gs://datalake-public-raw-hml-01";
const PREFIX = "ingestor";

const tables = [
        { 
            name: "cvm_fidc_reports",
            mode: "file_incremental",
            clusterBy: ["cnpj_fundo","reference_date"],
        },
        { 
            name: "cvm_fidc_cotas",
            mode: "file_incremental",
            clusterBy: ["cnpj_fundo","reference_date"],
        },
        { 
            name: "cvm_cri_reports",
            mode: "file_incremental",
            clusterBy: ["cnpj_emissor","nr_emissao"],
        },
        { 
            name: "cvm_cra_reports",
            mode: "file_incremental",
            clusterBy: ["cnpj_emissor","nr_emissao"],
        },
        {
            name: "cvm_fi_holdings",
            mode: "file_incremental",
            clusterBy: ["cnpj_fundo"],
        },
        { 
            name: "cvm_fi_cadastral",
            mode: "snapshot",
            clusterBy: ["cnpj_fundo"],
        },
        {
            name: "cvm_fi_docs_eventual",
            mode: "file_incremental",
            clusterBy: ["cnpj_fundo"],
        },
        { 
            name: "cvm_debenture_offers",
            mode: "snapshot",
            clusterBy: ["cnpj_emissor","nr_emissao"],
        },
        { 
            name: "cvm_sre_offerings",
            mode: "snapshot",
            clusterBy: ["cnpj_emissor","nr_emissao"],
        },
        { 
            name: "cvm_dfin_docs",
            mode: "file_incremental",
            clusterBy: ["cnpj_emissor","nr_emissao"],
        },
        //{ name: "cvm_fi_docs_eventual" },
        {
            name: "b3_negociacoes",
            mode: "file_incremental",
            partitionBy: "DATE_TRUNC(data_referencia, YEAR)",
            clusterBy: ["codigo_isin"],
        },

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
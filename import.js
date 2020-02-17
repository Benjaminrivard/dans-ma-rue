const config = require("config");
const csv = require("csv-parser");
const fs = require("fs");
const { Client } = require("@elastic/elasticsearch");
const indexName = config.get("elasticsearch.index_name");

async function run() {
  // Create Elasticsearch client
  const client = new Client({ node: config.get("elasticsearch.uri") });

  // Création de l'indice
  client.indices.create({ index: indexName }, (err, resp) => {
    if (err) {
      console.trace(err.message);
    } else {
      client.indices.putMapping({
        index: indexName,
        body: {
          properties: {
            location: {
              type: "geo_point"
            }
          }
        }
      });
    }
  });

  // Read CSV file
  const signalements = [];
  fs.createReadStream("dataset/dans-ma-rue.csv")
    .pipe(
      csv({
        separator: ";"
      })
    )
    .on("data", data => {
      const signalement = {
        timestamp: data.DATEDECL,
        object_id: data.OBJECTID,
        annee_declaration: data["ANNEE DECLARATION"],
        mois_declaration: data["MOIS DECLARATION"],
        type: data.TYPE,
        sous_type: data.SOUSTYPE,
        code_postal: data.CODE_POSTAL,
        ville: data.VILLE,
        arrondissement: data.ARRONDISSEMENT,
        prefixe: data.PREFIXE,
        intervenant: data.INTERVENANT,
        conseil_de_quartier: data["CONSEIL DE QUARTIER"],
        location: data.geo_point_2d
      };
      signalements.push(signalement);
    })
    .on("end", () => {
      sendBulkInsertQuery(client, signalements)
        .catch(e => {
          console.error(e);
        })
        .then(() => {
          console.log("Tous les signalements insérés");
          client.close();
        });
    });
}

// Fonction utilitaire permettant de découper les données en chunks puis d'envoyer les requêtes
async function sendBulkInsertQuery(client, signalements) {
  return new Promise(async (resolve, reject) => {
    while (signalements.length) {
      const { body: bulkResponse } = await client.bulk(
        createBulkInsertQuery(signalements.splice(0, 20000))
      );
      if (bulkResponse.errors) {
        reject();
      }
      console.log(
        `${
          bulkResponse.items.length !== undefined
            ? bulkResponse.items.length
            : 0
        } signalements insérés`
      );
      resolve();
    }
  });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(signalements) {
  const body = signalements.reduce((acc, signalement) => {
    const {
      timestamp,
      annee_declaration,
      mois_declaration,
      type,
      sous_type,
      code_postal,
      ville,
      arrondissement,
      prefixe,
      intervenant,
      conseil_de_quartier,
      location
    } = signalement;
    acc.push({
      index: { _index: indexName, _type: "_doc", _id: signalement.object_id }
    });
    acc.push({
      timestamp,
      annee_declaration,
      mois_declaration,
      type,
      sous_type,
      code_postal,
      ville,
      arrondissement,
      prefixe,
      intervenant,
      conseil_de_quartier,
      location
    });
    return acc;
  }, []);

  return { body };
}

run().catch(console.error);

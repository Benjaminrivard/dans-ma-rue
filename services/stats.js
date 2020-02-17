const config = require("config");
const indexName = config.get("elasticsearch.index_name");

// Compter le nombre d'anomalies par arondissement
exports.statsByArrondissement = (client, callback) => {
  client
    .search({
      index: indexName,
      body: {
        size: 0,
        aggs: {
          arrondissements: {
            terms: {
              field: "arrondissement.keyword",
              size: 20
            }
          }
        }
      }
    })
    .then(response =>
      callback(
        response.body.aggregations.arrondissements.buckets.map(a => {
          return {
            arrondissement: a.key,
            count: a.doc_count
          };
        })
      )
    );
};

exports.statsByType = (client, callback) => {
  // TODO Trouver le top 5 des types et sous types d'anomalies
  callback([]);
};

exports.statsByMonth = (client, callback) => {
  // TODO Trouver le top 10 des mois avec le plus d'anomalies
  callback([]);
};

exports.statsPropreteByArrondissement = (client, callback) => {
  // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
  callback([]);
};

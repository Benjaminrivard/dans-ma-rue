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
        response.body.aggregations.arrondissements.buckets.map(a =>
          formatArrondissement(a)
        )
      )
    )
    .catch(e => console.error(e));
};

// Trouver le top 5 des types et sous types d'anomalies
exports.statsByType = (client, callback) => {
  client
    .search({
      index: indexName,
      body: {
        size: 0,
        aggs: {
          types: {
            terms: {
              field: "type.keyword",
              size: 5
            },
            aggs: {
              sous_types: {
                terms: {
                  field: "sous_type.keyword",
                  size: 5
                }
              }
            }
          }
        }
      }
    })
    .then(response =>
      callback(
        response.body.aggregations.types.buckets.map(t => {
          return {
            type: t.key,
            count: t.doc_count,
            sous_types: t.sous_types.buckets.map(st => {
              return {
                sous_type: st.key,
                count: st.doc_count
              };
            })
          };
        })
      )
    )
    .catch(e => console.error(e));
};

// Trouver le top 10 des mois avec le plus d'anomalies
exports.statsByMonth = (client, callback) => {
  client
    .search({
      index: indexName,
      body: {
        size: 0,
        aggs: {
          months: {
            terms: {
              field: "mois_annee_declaration.keyword",
              size: 10
            }
          }
        }
      }
    })
    .then(response =>
      callback(
        response.body.aggregations.months.buckets.map(a => {
          return {
            month: a.key,
            count: a.doc_count
          };
        })
      )
    )
    .catch(e => console.error(e));
};

// Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
exports.statsPropreteByArrondissement = (client, callback) => {
  client
    .search({
      index: indexName,
      body: {
        size: 0,
        query: {
          bool: {
            must: {
              match: { type: "Propreté" }
            }
          }
        },
        aggs: {
          arrondissements: {
            terms: {
              field: "arrondissement.keyword",
              size: 3
            }
          }
        }
      }
    })
    .then(response =>
      callback(
        response.body.aggregations.arrondissements.buckets.map(a =>
          formatArrondissement(a)
        )
      )
    )
    .catch(error => console.error(error.body.error));
};

function formatArrondissement(a) {
  return {
    arrondissement: a.key,
    count: a.doc_count
  };
}

const config = require("config");
const indexName = config.get("elasticsearch.index_name");

// Compter le nombre d'anomalies entre deux dates
exports.count = (client, from, to, callback) => {
  client
    .count({
      index: indexName,
      body: {
        query: {
          range: {
            timestamp: {
              gte: from,
              lt: to
            }
          }
        }
      }
    })
    .then(resp =>
      callback({
        count: resp.body.count
      })
    )
    .catch(e => console.error(e));
};

// Compter le nombre d'anomalies autour d'un point géographique, dans un rayon donné
exports.countAround = (client, lat, lon, radius, callback) => {
  client
    .search({
      index: indexName,
      body: {
        query: {
          bool: {
            must: {
              match_all: {}
            },
            filter: {
              geo_distance: {
                distance: radius,
                location: {
                  lat: lat,
                  lon: lon
                }
              }
            }
          }
        }
      }
    })
    .then(resp => {
      callback({
        count: resp.body.hits.total.value
      });
    })
    .catch(e => console.error(e));
};

const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const heroesIndexName = 'heroes';

async function run () {
    const esClient = new Client({ node: 'http://localhost:9200' });

    // Read CSV file
    let line_index = 0;
    let heroes=[];
    let promises = [];

    // Créer mapping pour la suggestion
    esClient.indices.create({
        index: heroesIndexName,
        body: {
            "mappings": {
                "properties" : {
                    "suggest" : {
                        "type" : "completion"
                    },
                    "title" : {
                        "type": "keyword"
                    }
                }
            }
        }
    }, function (err, resp, respcode) {
        console.log("Response code", respcode);
    });


    fs.createReadStream('./all-heroes.csv')
        .pipe(csv({
            separator: ','
        }))
        .on('data', (data) => {
            // Increment sub array index
            if(line_index%10000===0 && line_index!==0){
                // Copy array
                const heroesDuplicate = [...heroes];
                
                // Empty array
                heroes.length = 0;

                promises.push(new Promise((resolve, reject) => {
                    esClient.bulk(createBulkInsertQuery(heroesDuplicate), (err, resp) => {
                        if (err){
                            console.trace(err.message);
                            reject();
                        }
                            
                        else {
                            console.log(`Inserted ${resp.body.items.length} heroes`);
                            resolve();
                        }
                    })
                }));
            }

            heroes.push({
                "id": data.id,
                "suggest": [{input:data.name,weight:10},{input:data.aliases,weight:8},{input:data.identity,weight:8},{input:data.description,weight:5},{input:data.partners,weight:4}],
                "imageUrl":data.imageUrl,
                "name": data.name,
                "universe":data.universe,
                "gender":data.gender,
                "description":data.description,
                "identity": {
                    "secretIdentities": data.secretIdentities.split(',').map(s=>s.trim()),
                    "birthPlace": data.birthPlace,
                    "occupation": data.occupation,
                    "aliases": data.aliases.split(',').map(s=>s.trim()),
                    "alignment": data.alignment,
                    "firstAppearance": data.firstAppearance,
                    "yearAppearance": data.yearAppearance,
                    "universe": data.universe
                },
                "partners":data.partners.split(',').map(s=>s.trim())
            });            

            // Increment index
            line_index++;
        })
        .on('end', () => {
            // Make last insert
            Promise.all(promises).then(() => {
                esClient.bulk(createBulkInsertQuery(heroes), (err, resp) => {
                    if (err)
                        console.trace(err.message);
                    else
                        console.log(`Inserted ${resp.body.items.length} heroes`);

                    console.log("Closing connection");
                    esClient.close();
                })
            });
        });

        function createBulkInsertQuery(heroes) {
            const body = heroes.reduce((he, hero) => {
              he.push({ index: { _index: heroesIndexName, _type: '_doc', _id: hero.object_id } });
              he.push(hero);
              return he;
            }, []);
            
            return { 
                body: body,
                refresh: "wait_for"
            };
        }
}

run().catch(console.error);

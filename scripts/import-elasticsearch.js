const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch')
const heroesIndexName = 'heroes'

async function run () {
    const esClient = new Client({ node: 'http://localhost:9200' })

    // Read CSV file
    let line_index = 0;
    let heroes=[]
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
            if(line_index%10000===0 && line_index!=0){
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
                "name": data.name,
                "birth_date": data.birth_date,
                "description":data.description,
                "imageUrl":data.imageUrl,
                "backgroundImageUrl":data.backgroundImageUrl,
                "externalLink":data.externalLink,
                "secretIdentities":data.secretIdentities,
                "birthPlace":data.birthPlace,
                "occupation":data.occupation,
                "aliases":data.aliases,
                "alignment":data.alignment,
                "firstAppearance":data.firstAppearance,
                "universe":data.universe,
                "gender":data.gender,
                "race":data.race,
                "type":data.type,
                "height":data.height,
                "weight":data.weight,
                "eyeColor":data.eyeColor,
                "hairColor":data.hairColor,
                "teams":data.teams,
                "powers":data.powers,
                "partners":data.partners,
                "intelligence":data.intelligence,
                "strength":data.strength,
                "speed":data.speed,
                "durability":data.durability,
                "power":data.power,
                "combat":data.combat,
                "creators":data.creators
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
              he.push({ index: { _index: heroesIndexName, _type: '_doc', _id: hero.object_id } })
              he.push(hero)
              return he
            }, []);
            
            return { 
                body: body,
                refresh: "wait_for"
            };
        }
}

run().catch(console.error);

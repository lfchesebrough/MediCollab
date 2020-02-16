var viz;
function draw(search) {
    var config = {
        container_id: "viz",
        server_url: "bolt://54.236.34.188:7687",
        server_user: "neo4j",
        server_password: "insight",
        labels: {
            "Endocrinology": {
                "caption": "NPPES_PROVIDER_LAST_ORG_NAME",
                "size": "TOTAL_UNIQUE_BENES"
            }
        },
        relationships: {
            "REFERRED_TO": {
                "thickness": "patient_count",
                "caption": false
            }
        },
        initial_cypher: 'MATCH (n:'+search+')-[r:REFERRED_TO]->(p)-[c:REFERRED_TO]->(q) WHERE r.average_day_wait < 10 and c.average_day_wait < 10 and r.patient_count > 20 and c.patient_count > 20 RETURN * LIMIT 100'
    };

    viz = new NeoVis.default(config);
    viz.render();
    console.log(viz);
}

function stabilize() {
    viz.stabilize();
}

var viz;
function draw(search, zip) {
    var config = {
        container_id: "viz",
        server_url: "bolt://54.236.34.188:7687",
        server_user: "neo4j",
        server_password: "insight",
        labels: {
            "Family Practice":{
                "size": "pagerank",
                "community": "community",
                "title_properties": [
                            "Last_Name",
                            "First_Name",
                            "NPI",
                            "Cost_Per_Patient",
                            "Quality_Score",
                            "Zip_Code"
                        ]
            },
            "Endocrinology":{
                "size": "pagerank",
                "community": "community",
                "title_properties": [
                            "Last_Name",
                            "First_Name",
                            "NPI",
                            "Cost_Per_Patient",
                            "Quality_Score",
                            "Zip_Code"
                        ]
            },
            "Dermatology":{
                "size": "pagerank",
                "community": "community",
                "title_properties": [
                            "Last_Name",
                            "First_Name",
                            "NPI",
                            "Cost_Per_Patient",
                            "Quality_Score",
                            "Zip_Code"]
            },
            "Nephrology":{
                "size": "pagerank",
                "community": "community",
                "title_properties": [
                            "Last_Name",
                            "First_Name",
                            "NPI",
                            "Cost_Per_Patient",
                            "Quality_Score",
                            "Zip_Code"
                        ]
            },
            "Gastroenterology":{
                "size": "pagerank",
                "community": "community",
                "title_properties": [
                            "Last_Name",
                            "First_Name",
                            "NPI",
                            "Cost_Per_Patient",
                            "Quality_Score",
                            "Zip_Code"
                        ]
            },
            "Physical Therapist in Private Practice":{
                "size": "pagerank",
                "community": "community",
                "title_properties": [
                            "Last_Name",
                            "First_Name",
                            "NPI",
                            "Cost_Per_Patient",
                            "Quality_Score",
                            "Zip_Code"
                        ]
            },
            "Obstetrics & Gynecology":{
                "size": "pagerank",
                "community": "community",
                "title_properties": [
                            "Last_Name",
                            "First_Name",
                            "NPI",
                            "Cost_Per_Patient",
                            "Quality_Score",
                            "Zip_Code"
                        ]
            },
            "Orthopedic Surgery":{
                "size": "pagerank",
                "community": "community",
                "title_properties": [
                            "Last_Name",
                            "First_Name",
                            "NPI",
                            "Cost_Per_Patient",
                            "Quality_Score",
                            "Zip_Code"
                        ]
            },
            "Internal Medicine":{
                "size": "pagerank",
                "community": "community",
                "title_properties": [
                            "Last_Name",
                            "First_Name",
                            "NPI",
                            "Cost_Per_Patient",
                            "Quality_Score",
                            "Zip_Code"
                        ]
            },            "Psychiatry":{
                "size": "pagerank",
                "community": "community",
                "title_properties": [
                            "Last_Name",
                            "First_Name",
                            "NPI",
                            "Cost_Per_Patient",
                            "Quality_Score",
                            "Zip_Code"
                        ]
            }
        },
        relationships: {
            "REFERRED_TO": {
                "thickness": "patient_count",
                "caption": false
            }
        },
        initial_cypher: 'MATCH (n:`'+search+'` {Zip_Code:"'+zip+'"})-[r]-(m) WHERE m:`Family Practice` OR m:`Physical Therapist in Private Practice`OR m:`Obstetrics & Gynecology`$
    };

    viz = new NeoVis.default(config);
    viz.render();
    console.log(viz);
}

function stabilize() {
    viz.stabilize();
}


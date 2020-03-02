from wikiTrendApp import app
from flask import flash, request, render_template, redirect

from py2neo import Graph
from wikiTrendApp.neo4j_connector import neo4jConnector

@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        search = request.form['search']
        zip = request.form['zip']

        return search_results(search, zip)

    return render_template("index.html")

@app.route('/results')
def search_results(search, zip):
    #results = pageRank(search)
    results = specialist(search, zip)
    #if not results:
        #return redirect('/')

    #else:
    return render_template("results.html", len=len(results), results=results, search=search, zip=zip)

    #return render_template("results.html", search=search)

def specialist(search, zip):
    gc = neo4jConnector().graph

    temp = gc.run(
    '''
    MATCH (n:'''+search+''' {Zip_Code:"'''+zip+'''"})-[r:REFERRED_TO]-(m)
    where n.community = m.community
    return n.Last_Name, n.First_Name, n.NPI, n.Quality_Score, n.Cost_Per_Patient, toInteger(avg(m.Cost_Per_Patient)), n.pagerank order by n.pagerank desc'''
    ).data()

    results = []
    resultsName = set()

    for i in range(len(temp)):
        # if (len(resultsName) > 5):
        #     break
        # if temp[i]['doc'] not in resultsName:
        results.append(temp[i])
        #resultsName.add(temp[i]['doc'])

    return results

def pageRank(search):
    gc = neo4jConnector().graph
    temp = gc.run(
    '''
    CALL algo.pageRank.stream(
        'MATCH (:Link {name:"'''+search+'''"})-[*1]-(p:Link)
            RETURN DISTINCT id(p) AS id',
        'MATCH (p1:Link)-[r]->(p2:Link)
            RETURN id(p1) AS source, id(p2) AS target, r.OCCURENCE AS weight',
        {
        graph: 'cypher',
        iterations:10,
        dampingFactor:0.7
        })

    YIELD nodeId, score

    RETURN algo.asNode(nodeId).name AS page,score
    ORDER BY score DESC
    LIMIT 5
    '''
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)


    
    

@app.route('/api/ingest', methods=['POST'])
def ingest():
    data = request.get_json(force=True)
    s_id = str(data.get('search_id'))
    p_token = data.get('pulsar_token')
    date_from = data.get('from')
    date_to = data.get('to')
    
    def generate():
        total = 0
        for page in range(15): # Extended pages for 15-min deep dive
            query = """
            query($f:FilterInput!){
              results(filter:$f, limit:50, offset:"""+str(page*50)+"""){
                results {
                  content visibility engagement source publishedAt
                  analysis {
                    sentiment { label score }
                    emotions { label score }
                    topics { label }
                  }
                }
              }
            }
            """
            # Use dynamic date filters from the frontend
            vars = {"f": {"searchIds": [s_id], "dateFrom": date_from, "dateTo": date_to}}
            r = requests.post("https://data.pulsarplatform.com/graphql/trac", 
                             json={"query": query, "variables": vars}, 
                             headers={"Authorization": f"Bearer {p_token}"})
            
            # (Rest of ingestion logic remains the same...)

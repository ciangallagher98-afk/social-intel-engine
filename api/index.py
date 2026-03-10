from flask import Flask, request, jsonify, Response
import requests
import json
import time
from groq import Groq

app = Flask(__name__)
knowledge_base = {}

@app.route('/api/ingest', methods=['POST'])
def ingest():
    try:
        data = request.get_json(force=True)
        s_id = str(data.get('search_id'))
        p_token = data.get('pulsar_token')
        d_from = data.get('from')
        d_to = data.get('to')
        
        def generate():
            total = 0
            knowledge_base[s_id] = []
            
            # Initial Heartbeat to prove connection is live
            yield f"data: {json.dumps({'status': 'active', 'log': 'Connection Established...'})}\n\n"

            for page in range(20):
                offset = page * 50
                query = """
                query G($f:FilterInput!){
                  results(filter:$f, limit:50, offset:""" + str(offset) + """){
                    results {
                      content visibility engagement source publishedAt
                      analysis {
                        sentiment { label }
                        emotions { label }
                        topics { label }
                      }
                    }
                  }
                }
                """
                vars = {"f": {"searchIds": [s_id], "dateFrom": d_from, "dateTo": d_to}}
                
                try:
                    r = requests.post("https://data.pulsarplatform.com/graphql/trac", 
                                     json={"query": query, "variables": vars}, 
                                     headers={"Authorization": f"Bearer {p_token}"},
                                     timeout=45)
                    
                    if r.status_code != 200:
                        yield f"data: {json.dumps({'status': 'error', 'log': f'API Error: {r.status_code}'})}\n\n"
                        break

                    res_json = r.json()
                    # Check for GraphQL specific errors
                    if "errors" in res_json:
                        err_msg = res_json['errors'][0].get('message', 'Unknown GraphQL Error')
                        yield f"data: {json.dumps({'status': 'error', 'log': f'GraphQL: {err_msg}'})}\n\n"
                        break

                    batch = res_json.get('data', {}).get('results', {}).get('results', [])
                    
                    if not batch:
                        yield f"data: {json.dumps({'status': 'log', 'log': 'No more data found for this page.'})}\n\n"
                        break
                    
                    total += len(batch)
                    knowledge_base[s_id].extend(batch)
                    
                    progress = int(((page + 1) / 20) * 100)
                    yield f"data: {json.dumps({'status': 'ingesting', 'count': total, 'progress': progress, 'log': f'Collected {total} posts...'})}\n\n"
                    time.sleep(0.5)
                    
                except Exception as inner_e:
                    yield f"data: {json.dumps({'status': 'error', 'log': f'Network: {str(inner_e)}'})}\n\n"
                    break
                    
            yield f"data: {json.dumps({'status': 'complete', 'total': total})}\n\n"
        
        return Response(generate(), mimetype='text/event-stream')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/ask', methods=['POST'])
def ask():
    # ... (Ask logic remains same as previous working version)
    pass # Use previous ask logic here

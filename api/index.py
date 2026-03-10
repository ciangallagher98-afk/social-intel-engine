from flask import Flask, request, jsonify, Response
import requests
import json
import time
from groq import Groq

app = Flask(__name__)
knowledge_base = {}

def clean_string(text):
    if not text: return ""
    return text.replace('\u2028', ' ').replace('\u2029', ' ').encode('utf-8', 'ignore').decode('utf-8')

@app.route('/api/ingest', methods=['POST'])
def ingest():
    try:
        data = request.get_json(force=True)
        # FIX 1: Ensure Search ID is an Integer
        s_id = int(data.get('search_id'))
        p_token = data.get('pulsar_token')
        
        # FIX 2: Hard-code a known working ISO format for testing
        d_from = data.get('from').split('.')[0] + "Z" 
        d_to = data.get('to').split('.')[0] + "Z"

        def generate():
            total = 0
            knowledge_base[str(s_id)] = []
            yield f"data: {json.dumps({'status': 'active', 'log': f'Querying ID {s_id} from {d_from}...'})}\n\n"

            for page in range(10):
                offset = page * 50
                # FIX 3: Simplified GraphQL Template
                query = """
                query GetResults($f: FilterInput!) {
                  results(filter: $f, limit: 50, offset: %d) {
                    results {
                      content
                      source
                      analysis {
                        sentiment { label }
                        emotions { label }
                        topics { label }
                      }
                    }
                  }
                }
                """ % offset
                
                variables = {
                    "f": {
                        "searchIds": [s_id], 
                        "dateFrom": d_from, 
                        "dateTo": d_to
                    }
                }
                
                payload = json.dumps({"query": query, "variables": variables}).encode('utf-8')
                
                r = requests.post(
                    "https://data.pulsarplatform.com/graphql/trac", 
                    data=payload, 
                    headers={
                        "Authorization": f"Bearer {p_token}",
                        "Content-Type": "application/json; charset=utf-8"
                    },
                    timeout=45
                )
                
                res_json = r.json()
                
                # DEBUG: If 0 results, tell the user why
                if "errors" in res_json:
                    yield f"data: {json.dumps({'status': 'error', 'log': f'Pulsar Error: {res_json['errors'][0].get('message')}'})}\n\n"
                    break

                batch = res_json.get('data', {}).get('results', {}).get('results', [])
                
                if not batch:
                    if page == 0:
                        yield f"data: {json.dumps({'status': 'log', 'log': 'API returned 0 results. Check Date Range/ID.'})}\n\n"
                    break
                
                for post in batch:
                    post['content'] = clean_string(post.get('content', ''))
                
                total += len(batch)
                knowledge_base[str(s_id)].extend(batch)
                
                yield f"data: {json.dumps({'status': 'ingesting', 'count': total, 'progress': (page+1)*10, 'log': f'Fetched {len(batch)} posts...'})}\n\n"
                time.sleep(0.5)
                    
            yield f"data: {json.dumps({'status': 'complete', 'total': total})}\n\n"
        
        return Response(generate(), mimetype='text/event-stream')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Keep your /api/ask route as is...

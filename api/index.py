from flask import Flask, request, jsonify, Response
import requests
import json
import time
from groq import Groq

app = Flask(__name__)

# Global storage for the session (In-memory)
knowledge_base = {}

def sanitize_data(text):
    """
    Strips out \u2028, \u2029 and other non-latin-1 characters 
    that cause API stream crashes.
    """
    if not text:
        return ""
    # Replace line/paragraph separators with spaces and force UTF-8
    clean = text.replace('\u2028', ' ').replace('\u2029', ' ')
    return clean.encode('utf-8', 'ignore').decode('utf-8')

@app.route('/api/ingest', methods=['POST'])
def ingest():
    try:
        data = request.get_json(force=True)
        
        # 1. Extract and Format Parameters
        pulsar_token = data.get('pulsar_token')
        # Convert Search ID to Int to satisfy Pulsar GraphQL schema
        search_id = int(data.get('search_id')) 
        
        # Ensure dates are Pulsar-compliant ISO strings
        date_from = data.get('from').split('.')[0] + "Z"
        date_to = data.get('to').split('.')[0] + "Z"

        def generate_stream():
            total_indexed = 0
            knowledge_base[str(search_id)] = []
            
            yield f"data: {json.dumps({'status': 'active', 'log': 'Handshake Verified. Forcing UTF-8 Stream...'})}\n\n"

            # 2. Pagination Loop (Deep Ingestion)
            for page in range(20):
                offset = page * 50
                
                query = """
                query GetPulsarData($f: FilterInput!) {
                  results(filter: $f, limit: 50, offset: %d) {
                    results {
                      content
                      source
                      visibility
                      engagement
                      publishedAt
                      analysis {
                        sentiment { label score }
                        emotions { label score }
                        topics { label }
                      }
                    }
                  }
                }
                """ % offset

                variables = {
                    "f": {
                        "searchIds": [search_id],
                        "dateFrom": date_from,
                        "dateTo": date_to
                    }
                }

                # 3. Execute Pulsar Request
                try:
                    # We encode the payload to utf-8 manually to bypass system default encoding
                    payload = json.dumps({"query": query, "variables": variables}).encode('utf-8')
                    
                    response = requests.post(
                        "https://data.pulsarplatform.com/graphql/trac",
                        data=payload,
                        headers={
                            "Authorization": f"Bearer {pulsar_token}",
                            "Content-Type": "application/json; charset=utf-8"
                        },
                        timeout=45
                    )
                    
                    if response.status_code != 200:
                        yield f"data: {json.dumps({'status': 'error', 'log': f'HTTP {response.status_code}: Pulsar Rejected Request'})}\n\n"
                        break

                    res_json = response.json()
                    
                    # Handle GraphQL Internal Errors
                    if "errors" in res_json:
                        msg = res_json['errors'][0].get('message', 'GraphQL Logic Error')
                        yield f"data: {json.dumps({'status': 'error', 'log': f'Pulsar Logic: {msg}'})}\n\n"
                        break

                    batch = res_json.get('data', {}).get('results', {}).get('results', [])
                    
                    if not batch:
                        if page == 0:
                            yield f"data: {json.dumps({'status': 'log', 'log': 'Zero results found for this range/ID.'})}\n\n"
                        break

                    # 4. Sanitize and Store
                    for post in batch:
                        post['content'] = sanitize_data(post.get('content', ''))
                    
                    total_indexed += len(batch)
                    knowledge_base[str(search_id)].extend(batch)
                    
                    # Send Progress Update
                    progress = int(((page + 1) / 20) * 100)
                    yield f"data: {json.dumps({
                        'status': 'ingesting', 
                        'count': total_indexed, 
                        'progress': progress, 
                        'log': f'Captured {len(batch)} nodes (Total: {total_indexed})'
                    })}\n\n"
                    
                    time.sleep(0.4) # Throttle to prevent rate-limiting

                except Exception as e:
                    yield f"data: {json.dumps({'status': 'error', 'log': f'Stream Interrupted: {str(e)}'})}\n\n"
                    break

            yield f"data: {json.dumps({'status': 'complete', 'total': total_indexed})}\n\n"

        return Response(generate_stream(), mimetype='text/event-stream')

    except Exception as top_e:
        return jsonify({"error": str(top_e)}), 500

@app.route('/api/ask', methods=['POST'])
def ask():
    try:
        data = request.get_json(force=True)
        search_id = str(data.get('search_id'))
        user_query = data.get('question')
        groq_api_key = data.get('groq_key')

        # Retrieve indexed data
        raw_data = knowledge_base.get(search_id, [])
        if not raw_data:
            return jsonify({"answer": "No data found for this ID. Please run ingestion first."}), 400

        # 5. Context Compression for LLM
        # We take the top 500 items to fit in Llama-3's context window
        context = []
        for item in raw_data[:500]:
            analysis = item.get('analysis', {}) or {}
            context.append({
                "text": item.get('content', '')[:140],
                "sentiment": analysis.get('sentiment', {}).get('label', 'N/A'),
                "emotions": [e.get('label') for e in analysis.get('emotions', [])[:1]],
                "topics": [t.get('label') for t in analysis.get('topics', [])[:2]]
            })

        # 6. Groq Intelligence Synthesis
        client = Groq(api_key=groq_api_key)
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system", 
                    "content": "You are a Brand Intelligence Agent. Use the provided Emotion and Topic data to answer queries with statistical context. Be concise and professional."
                },
                {
                    "role": "user", 
                    "content": f"Context Data: {json.dumps(context)}\n\nUser Question: {user_query}"
                }
            ],
            temperature=0.3
        )

        return jsonify({"answer": completion.choices[0].message.content})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)

from flask import Flask, request, jsonify, Response
import requests
import json
import time
import sys
from groq import Groq

# Force the entire Python environment to prefer UTF-8
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

app = Flask(__name__)
knowledge_base = {}

def clean_string(text):
    """Strips out problematic encoding characters before they hit the JSON serializer."""
    if not text: return ""
    # Specifically targeting the \u2028 and \u2029 separators that caused your crash
    return text.replace('\u2028', ' ').replace('\u2029', ' ').encode('utf-8', 'ignore').decode('utf-8')

@app.route('/api/ingest', methods=['POST'])
def ingest():
    try:
        data = request.get_json(force=True)
        s_id = str(data.get('search_id'))
        p_token = data.get('pulsar_token')
        d_from, d_to = data.get('from'), data.get('to')
        
        def generate():
            total = 0
            knowledge_base[s_id] = []
            yield f"data: {json.dumps({'status': 'active', 'log': 'Connection Secured. Forcing UTF-8...'})}\n\n"

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
                    # We use 'data' instead of 'json' to have manual control over encoding
                    payload = json.dumps({"query": query, "variables": vars}).encode('utf-8')
                    
                    r = requests.post(
                        "https://data.pulsarplatform.com/graphql/trac", 
                        data=payload, 
                        headers={
                            "Authorization": f"Bearer {p_token}",
                            "Content-Type": "application/json; charset=utf-8"
                        },
                        timeout=45
                    )
                    
                    if r.status_code != 200:
                        yield f"data: {json.dumps({'status': 'error', 'log': f'HTTP {r.status_code}: API Rejected Request'})}\n\n"
                        break

                    # Interpret the response explicitly as UTF-8
                    r.encoding = 'utf-8'
                    res_json = r.json()
                    
                    batch = res_json.get('data', {}).get('results', {}).get('results', [])
                    if not batch: break
                    
                    # Clean the content field for every post
                    for post in batch:
                        post['content'] = clean_string(post.get('content', ''))
                    
                    total += len(batch)
                    knowledge_base[s_id].extend(batch)
                    
                    progress = int(((page + 1) / 20) * 100)
                    yield f"data: {json.dumps({'status': 'ingesting', 'count': total, 'progress': progress, 'log': f'Successfully Indexed {total} posts...'})}\n\n"
                    time.sleep(0.4)
                    
                except Exception as inner_e:
                    yield f"data: {json.dumps({'status': 'error', 'log': f'Encoding/Parsing Failure: {str(inner_e)}'})}\n\n"
                    break
                    
            yield f"data: {json.dumps({'status': 'complete', 'total': total})}\n\n"
        
        return Response(generate(), mimetype='text/event-stream')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/ask', methods=['POST'])
def ask():
    try:
        data = request.get_json(force=True)
        s_id, query, g_key = str(data.get('search_id')), data.get('question'), data.get('groq_key')
        
        dataset = knowledge_base.get(s_id, [])
        if not dataset: return jsonify({"error": "No data found. Please ingest first."}), 400

        # Create a compressed context for Groq
        context = []
        for p in dataset[:500]:
            an = p.get('analysis', {}) or {}
            context.append({
                "t": p.get('content', '')[:140],
                "s": an.get('sentiment', {}).get('label', 'neu'),
                "e": [e.get('label') for e in an.get('emotions', [])[:1]],
                "tp": [t.get('label') for t in an.get('topics', [])[:2]]
            })
            
        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are Gemini Intel. Provide strategic insights based on the Emotion and Topic data provided. Use bullet points."},
                {"role": "user", "content": f"Data: {json.dumps(context)}\n\nQuery: {query}"}
            ]
        )
        return jsonify({"answer": chat.choices[0].message.content})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

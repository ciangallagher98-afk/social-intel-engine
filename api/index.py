from flask import Flask, request, jsonify, Response
import requests
import json
import time
from groq import Groq

app = Flask(__name__)
# In-memory storage for the session
knowledge_base = {}

def deep_clean(obj):
    if isinstance(obj, str):
        return obj.encode('utf-8', 'ignore').decode('utf-8').replace('\u2028', ' ').replace('\u2029', ' ')
    elif isinstance(obj, list): return [deep_clean(item) for item in obj]
    elif isinstance(obj, dict): return {k: deep_clean(v) for k, v in obj.items()}
    return obj

@app.route('/api/ingest', methods=['POST'])
def ingest():
    try:
        data = request.get_json(force=True)
        s_id = str(data.get('search_id'))
        p_token = data.get('pulsar_token')
        
        def generate():
            total = 0
            for page in range(10): # Adjust range for 15-min deep dive
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
                vars = {"f": {"searchIds": [s_id], "dateFrom": "2026-01-01T00:00:00Z", "dateTo": "2026-03-10T23:59:59Z"}}
                r = requests.post("https://data.pulsarplatform.com/graphql/trac", 
                                 json={"query": query, "variables": vars}, 
                                 headers={"Authorization": f"Bearer {p_token}"})
                
                batch = r.json().get('data', {}).get('results', {}).get('results', [])
                if not batch: break
                
                total += len(batch)
                knowledge_base[s_id] = knowledge_base.get(s_id, []) + batch
                yield f"data: {json.dumps({'status': 'ingesting', 'count': total, 'progress': (page+1)*10})}\n\n"
                time.sleep(0.5)
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
        # Sample for context (Topics/Emotions included)
        context = []
        for p in dataset[:400]:
            analysis = p.get('analysis', {})
            context.append({
                "t": p.get('content', '')[:150],
                "sent": analysis.get('sentiment', {}).get('label'),
                "emo": [e.get('label') for e in analysis.get('emotions', [])[:1]],
                "top": [t.get('label') for t in analysis.get('topics', [])[:2]]
            })
            
        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are an MCP Data Assistant. Use Sentiment, Emotion, and Topic data to answer questions."},
                {"role": "user", "content": f"Dataset: {json.dumps(context)}\n\nUser Question: {query}"}
            ]
        )
        return jsonify({"answer": chat.choices[0].message.content})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

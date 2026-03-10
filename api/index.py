from flask import Flask, request, jsonify, Response
import requests
import json
import time
from groq import Groq

app = Flask(__name__)
knowledge_base = {}

def scrub(obj):
    if isinstance(obj, str):
        return obj.encode('utf-8', 'ignore').decode('utf-8').replace('\u2028', ' ').replace('\u2029', ' ')
    return obj

@app.route('/api/ingest', methods=['POST'])
def ingest():
    try:
        data = request.get_json(force=True)
        s_id, p_token = str(data.get('search_id')), data.get('pulsar_token')
        d_from, d_to = data.get('from'), data.get('to')
        
        def generate():
            total = 0
            knowledge_base[s_id] = []
            for page in range(20):
                query = """
                query($f:FilterInput!){
                  results(filter:$f, limit:50, offset:"""+str(page*50)+"""){
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
                r = requests.post("https://data.pulsarplatform.com/graphql/trac", 
                                 json={"query": query, "variables": vars}, 
                                 headers={"Authorization": f"Bearer {p_token}"})
                
                batch = r.json().get('data', {}).get('results', {}).get('results', [])
                if not batch: break
                total += len(batch)
                knowledge_base[s_id].extend(batch)
                yield f"data: {json.dumps({'status': 'ingesting', 'count': total, 'progress': (page+1)*5})}\n\n"
                time.sleep(0.3)
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
        context = []
        for p in dataset[:600]:
            analysis = p.get('analysis', {})
            context.append({
                "t": scrub(p.get('content', ''))[:150],
                "sent": analysis.get('sentiment', {}).get('label'),
                "emo": [e.get('label') for e in analysis.get('emotions', [])[:1]],
                "topics": [t.get('label') for t in analysis.get('topics', [])[:2]]
            })
        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are Gemini Intelligence. Answer based on the ingested Emotion/Topic data. Use Markdown for clarity."},
                {"role": "user", "content": f"Dataset: {json.dumps(context)}\n\nUser Question: {query}"}
            ]
        )
        return jsonify({"answer": chat.choices[0].message.content})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

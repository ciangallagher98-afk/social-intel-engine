from flask import Flask, request, jsonify, Response
import requests, json, time
from groq import Groq

app = Flask(__name__)
knowledge_base = {}

@app.route('/api/ingest', methods=['POST'])
def ingest():
    data = request.json
    s_id = str(data.get('search_id'))
    p_token = data.get('pulsar_token')
    
    def generate_progress():
        total_collected = 0
        limit = 50 
        
        # We fetch multiple pages to build the "15-minute" deep dive
        for page in range(20): 
            # NEW: Query includes sentiment, emotion, and topics
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
            variables = {"f": {"searchIds": [s_id], "dateFrom": "2026-01-01T00:00:00Z", "dateTo": "2026-03-10T23:59:59Z"}}
            
            res = requests.post("https://data.pulsarplatform.com/graphql/trac", 
                               json={"query": query, "variables": variables}, 
                               headers={"Authorization": f"Bearer {p_token}"})
            
            page_data = res.json().get('data', {}).get('results', {}).get('results', [])
            if not page_data: break
            
            total_collected += len(page_data)
            # Store everything in memory for the session
            knowledge_base[s_id] = knowledge_base.get(s_id, []) + page_data
            
            yield f"data: {json.dumps({'status': 'ingesting', 'count': total_collected, 'progress': (page+1)*5})}\n\n"
            time.sleep(0.5)
            
        yield f"data: {json.dumps({'status': 'complete', 'total': total_collected})}\n\n"

    return Response(generate_progress(), mimetype='text/event-stream')

@app.route('/api/ask', methods=['POST'])
def ask():
    data = request.json
    s_id, query, g_key = str(data.get('search_id')), data.get('question'), data.get('groq_key')

    dataset = knowledge_base.get(s_id, [])
    if not dataset: return jsonify({"error": "Ingest data first."}), 400

    # We pack the context with the new fields
    # We take the top 500 most impactful posts for the specific answer
    context = []
    for p in dataset[:500]:
        analysis = p.get('analysis', {})
        context.append({
            "text": p.get('content', '')[:150],
            "src": p.get('source'),
            "sent": analysis.get('sentiment', {}).get('label'),
            "emo": [e.get('label') for e in analysis.get('emotions', [])[:2]],
            "topics": [t.get('label') for t in analysis.get('topics', [])[:3]]
        })

    client = Groq(api_key=g_key)
    chat = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": "You are a Brand Strategist. Use the provided Sentiment, Emotion, and Topic data to answer queries with high statistical confidence. Always mention specific emotions or recurring topics if relevant."},
            {"role": "user", "content": f"Context: {json.dumps(context)}\n\nQuery: {query}"}
        ]
    )
    return jsonify({"answer": chat.choices[0].message.content})

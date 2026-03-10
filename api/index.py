from flask import Flask, request, jsonify
import requests
import json
from groq import Groq

app = Flask(__name__)

# Persistent in-memory storage for the session
knowledge_base = {}

def clean_text(text):
    """Prevents encoding crashes by stripping illegal characters."""
    if not text: return ""
    return text.replace('\u2028', ' ').replace('\u2029', ' ').encode('utf-8', 'ignore').decode('utf-8')

@app.route('/api/ingest', methods=['POST'])
def ingest():
    try:
        data = request.get_json(force=True)
        
        # Accept Hex ID as string to prevent int() conversion errors
        s_id = str(data.get('search_id')).strip()
        p_token = data.get('pulsar_token')
        
        # Dates from UI (ISO 8601)
        d_from = data.get('from')
        d_to = data.get('to')

        # NESTED SCHEMA FIX: Arguments belong to the inner results field
        query = """
        query GetPulsarData($f: FilterInput!) {
          results(filter: $f) {
            results(limit: 250, offset: 0, sort: { field: VISIBILITY, order: DESC }) {
              content
              source
              visibility
              engagements
              sentiment
              emotions
              topics
            }
          }
        }
        """
        
        variables = {
            "f": {
                "searchIds": [s_id],
                "dateFrom": d_from,
                "dateTo": d_to
            }
        }
        
        # Force UTF-8 encoding for the payload
        payload = json.dumps({"query": query, "variables": variables}).encode('utf-8')
        
        r = requests.post(
            "https://data.pulsarplatform.com/graphql/trac",
            data=payload,
            headers={
                "Authorization": f"Bearer {p_token}",
                "Content-Type": "application/json; charset=utf-8"
            },
            timeout=60
        )
        
        res_json = r.json()
        
        if "errors" in res_json:
            error_msg = res_json['errors'][0].get('message', 'GraphQL Error')
            return jsonify({"error": error_msg}), 400

        # Targeted extraction: data -> results (wrapper) -> results (list)
        batch = res_json.get('data', {}).get('results', {}).get('results', [])
        
        if not batch:
            return jsonify({
                "status": "empty", 
                "message": "Pulsar returned 0 results. Check ID/Dates."
            })

        for post in batch:
            post['content'] = clean_text(post.get('content', ''))
            
        knowledge_base[s_id] = batch
        
        return jsonify({
            "status": "success", 
            "count": len(batch),
            "log": f"Successfully indexed {len(batch)} visibility-prioritized nodes."
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/ask', methods=['POST'])
def ask():
    try:
        data = request.get_json(force=True)
        s_id = str(data.get('search_id')).strip()
        query = data.get('question')
        g_key = data.get('groq_key')

        dataset = knowledge_base.get(s_id, [])
        if not dataset:
            return jsonify({"answer": "Error: Knowledge base empty. Please run ingestion."}), 400

        # Pack top 150 most visible posts as context for Groq
        context = []
        for p in dataset[:150]:
            context.append({
                "text": p.get('content', '')[:140],
                "reach": p.get('visibility'),
                "sentiment": p.get('sentiment'),
                "emotions": p.get('emotions'),
                "topics": p.get('topics')
            })
            
        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are Gemini Intelligence. Analyze these high-visibility posts. Group insights by Emotion, Topic, and Reach using Markdown."},
                {"role": "user", "content": f"Data: {json.dumps(context)}\n\nQuery: {query}"}
            ],
            temperature=0.3
        )
        return jsonify({"answer": chat.choices[0].message.content})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)

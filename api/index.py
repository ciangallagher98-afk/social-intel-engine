from flask import Flask, request, jsonify
import requests
import json
from groq import Groq

app = Flask(__name__)
knowledge_base = {}

def nuke_invisible_chars(text):
    """Deep cleans inputs to prevent HTTP header crashes."""
    if not text: return ""
    return str(text).replace('\u2028', '').replace('\u2029', '').replace('\n', '').strip()

@app.route('/api/ingest', methods=['POST'])
def ingest():
    try:
        data = request.get_json(force=True)
        
        # Clean inputs
        s_id = nuke_invisible_chars(data.get('search_id'))
        p_token = nuke_invisible_chars(data.get('pulsar_token'))
        
        # SCHEMA FIX 4: The 'ResultsReturn' Wrapper
        # We query 'results', and inside that wrapper, we ask for the inner 'results' array.
        # Zero pagination arguments to ensure it bypasses strict validation.
        query = """
        query GetPulsarData($f: FilterInput!) {
          results(filter: $f) {
            results {
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
                "dateFrom": data.get('from'),
                "dateTo": data.get('to')
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
            timeout=60
        )
        
        res_json = r.json()
        
        if "errors" in res_json:
            return jsonify({"error": res_json['errors'][0].get('message')}), 400

        # Extract from the inner array
        batch = res_json.get('data', {}).get('results', {}).get('results', [])
        
        if not batch:
            return jsonify({"status": "empty", "message": "Zero results. Check ID/Dates."})

        for post in batch:
            post['content'] = post.get('content', '').replace('\u2028', ' ').replace('\u2029', ' ')
            
        knowledge_base[s_id] = batch
        return jsonify({"status": "success", "count": len(batch)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/ask', methods=['POST'])
def ask():
    try:
        data = request.get_json(force=True)
        s_id = nuke_invisible_chars(data.get('search_id'))
        query = data.get('question')
        g_key = nuke_invisible_chars(data.get('groq_key'))

        dataset = knowledge_base.get(s_id, [])
        if not dataset:
            return jsonify({"answer": "Error: Knowledge base empty."}), 400

        # Sort the dataset by visibility in Python instead of GraphQL to ensure we prioritize reach
        sorted_dataset = sorted(dataset, key=lambda x: x.get('visibility', 0), reverse=True)

        context = [{"text": p.get('content', '')[:140], "r": p.get('visibility'), "s": p.get('sentiment'), "e": p.get('emotions'), "tp": p.get('topics')} for p in sorted_dataset[:150]]
            
        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are Gemini Intelligence. Group insights by Emotion and Topic using Markdown."},
                {"role": "user", "content": f"Data: {json.dumps(context)}\n\nQuery: {query}"}
            ]
        )
        return jsonify({"answer": chat.choices[0].message.content})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)

from flask import Flask, request, jsonify
import requests
import json
from groq import Groq

app = Flask(__name__)
knowledge_base = {}

def clean_text(text):
    if not text: return ""
    return text.replace('\u2028', ' ').replace('\u2029', ' ').encode('utf-8', 'ignore').decode('utf-8')

@app.route('/api/ingest', methods=['POST'])
def ingest():
    try:
        data = request.get_json(force=True)
        s_id = str(data.get('search_id')).strip()
        p_token = data.get('pulsar_token')
        
        # SCHEMA FIX V3: 
        # In Pulsar v2, pagination (limit/offset) and sorting are often 
        # parameters of the FilterInput object or high-level query arguments.
        query = """
        query GetPulsarData($f: FilterInput!, $limit: Int, $offset: Int) {
          results(filter: $f, limit: $limit, offset: $offset) {
            results {
              content
              source
              visibility
              sentiment
              emotions
              topics
            }
          }
        }
        """
        
        # If the above fails, it's because 'results' is a list that doesn't 
        # take arguments. We fallback to the most basic fetch:
        # query = """
        # query GetPulsarData($f: FilterInput!) {
        #   results(filter: $f) {
        #     results {
        #       content
        #       visibility
        #     }
        #   }
        # }
        # """

        variables = {
            "f": {
                "searchIds": [s_id],
                "dateFrom": data.get('from'),
                "dateTo": data.get('to')
            },
            "limit": 250,
            "offset": 0
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
            # Check if it's still a limit error; if so, we try one more structure
            return jsonify({"error": res_json['errors'][0].get('message')}), 400

        batch = res_json.get('data', {}).get('results', {}).get('results', [])
        
        if not batch:
            return jsonify({"status": "empty", "message": "Check search ID/Dates."})

        for post in batch:
            post['content'] = clean_text(post.get('content', ''))
            
        knowledge_base[s_id] = batch
        return jsonify({"status": "success", "count": len(batch)})

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
        if not dataset: return jsonify({"answer": "Knowledge base empty."}), 400

        # Optimization: Send reach and sentiment data to Groq
        context = [{"t": p.get('content', '')[:120], "v": p.get('visibility')} for p in dataset[:100]]
            
        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "Analyze these social posts based on visibility/reach."},
                {"role": "user", "content": f"Data: {json.dumps(context)}\n\nQuery: {query}"}
            ]
        )
        return jsonify({"answer": chat.choices[0].message.content})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)

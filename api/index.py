from flask import Flask, request, jsonify
import requests
from groq import Groq
import json

app = Flask(__name__)

@app.route('/api/analyze', methods=['POST'])
def analyze():
    try:
        data = request.json
        p_token = data.get('pulsar_token')
        g_key = data.get('groq_key')
        s_id = str(data.get('search_id')) 
        
        p_start = f"{data.get('date_from')}T00:00:00Z"
        p_end = f"{data.get('date_to')}T23:59:59Z"

        # UPDATED: Added publishedAt to the query
        query = """
        query GetResults($filters:FilterInput!){
           results (filter:$filters){
               results { content visibility sentiment emotion engagement url publishedAt }
           }
        }
        """
        variables = {"filters": {"searchIds": [s_id], "dateFrom": p_start, "dateTo": p_end}}
        
        pulsar_res = requests.post(
            "https://data.pulsarplatform.com/graphql/trac", 
            json={"query": query, "variables": variables}, 
            headers={"Authorization": f"Bearer {p_token}", "Content-Type": "application/json"}
        )
        
        p_json = pulsar_res.json()
        if 'errors' in p_json:
            return jsonify({"error": p_json['errors'][0]['message']}), 400

        posts = p_json.get('data', {}).get('results', {}).get('results', [])
        if not posts:
            return jsonify({"error": "No data found."}), 404

        # Map data including the new timestamp
        context_items = [{
            "text": p.get('content', '')[:180], 
            "sent": p.get('sentiment'), 
            "emo": p.get('emotion'), 
            "impact": p.get('visibility', 0), 
            "date": p.get('publishedAt'), # NEW FIELD
            "url": p.get('url', '')
        } for p in posts[:50]]

        client = Groq(api_key=g_key)
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": """You are a Strategic Time-Series Analyst. 
                Use the 'date' field to identify why volume spikes occurred. 
                Correlate sentiment shifts with specific days.
                Return JSON with 'summary' (include date-based insights) and 'categories' array."""},
                {"role": "user", "content": f"Goal: {data.get('prompt')}\n\nData: {json.dumps(context_items)}"}
            ]
        )

        return jsonify(json.loads(completion.choices[0].message.content))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

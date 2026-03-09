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
        s_id = data.get('search_id')
        user_prompt = data.get('prompt')
        date_from = data.get('date_from')
        date_to = data.get('date_to')

        p_start = f"{date_from}T00:00:00Z"
        p_end = f"{date_to}T23:59:59Z"

        # UPDATED QUERY: Added visibility and contentUrl
        query = """
        query GetResults($filters:FilterInput!){
           results (filter:$filters){
               results { 
                   content 
                   source 
                   visibility 
                   contentsUrl
               }
           }
        }
        """
        variables = {"filters": {"searchIds": [s_id], "dateFrom": p_start, "dateTo": p_end}}
        headers = {"Authorization": f"Bearer {p_token}", "Content-Type": "application/json"}
        
        pulsar_res = requests.post("https://data.pulsarplatform.com/graphql/trac", 
                                    json={"query": query, "variables": variables}, headers=headers)
        
        posts = pulsar_res.json().get('data', {}).get('results', {}).get('results', [])

        if not posts:
            return jsonify({"error": "No data found."}), 404

        # Prepare data for AI - keeping visibility for context
        context_items = []
        for p in posts[:50]: # Increased to 50 for better evidence selection
            context_items.append({
                "text": p.get('content')[:200],
                "impact": p.get('visibility', 0),
                "url": p.get('contentUrl', '')
            })

        client = Groq(api_key=g_key)
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": """Analyze data and return JSON. 
                Identify categories. For each category, provide:
                1. 'name', 'count', 'boolean'
                2. 'impact_score': Average visibility for this category.
                3. 'evidence': The URL of the single most representative/high-impact post."""},
                {"role": "user", "content": f"Task: {user_prompt}\n\nData: {json.dumps(context_items)}"}
            ]
        )

        return jsonify(json.loads(completion.choices[0].message.content))
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

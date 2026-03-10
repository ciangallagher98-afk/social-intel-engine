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

        query = """
        query GetResults($filters:FilterInput!){
           results (filter:$filters){
               results { content visibility engagement url publishedAt source }
           }
        }
        """
        variables = {"filters": {"searchIds": [s_id], "dateFrom": p_start, "dateTo": p_end}}
        
        pulsar_res = requests.post(
            "https://data.pulsarplatform.com/graphql/trac", 
            json={"query": query, "variables": variables}, 
            headers={"Authorization": f"Bearer {p_token}", "Content-Type": "application/json"}
        )
        
        posts = pulsar_res.json().get('data', {}).get('results', {}).get('results', [])
        if not posts: return jsonify({"error": "No data found."}), 404

        # Calculate SOV before slicing
        sources = [p.get('source', 'Unknown') for p in posts]
        sov = {s: round((sources.count(s) / len(sources)) * 100) for s in set(sources)}

        # Sort by impact and send the most important context
        sorted_posts = sorted(posts, key=lambda x: x.get('visibility', 0), reverse=True)
        context = [{"text": p.get('content')[:200], "impact": p.get('visibility'), "src": p.get('source'), "date": p.get('publishedAt')} for p in sorted_posts[:50]]

        client = Groq(api_key=g_key)
        completion = client.chat.create(
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a Strategy Consultant. Return JSON: {executive_summary, themes: [{title, summary, impact_level, offensive_play, defensive_play}]}"},
                {"role": "user", "content": f"User Goal: {data.get('prompt')}\n\nData: {json.dumps(context)}"}
            ]
        )
        
        result = json.loads(completion.choices[0].message.content)
        result['sov'] = sov # Attach SOV data for the UI
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

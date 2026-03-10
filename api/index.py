from flask import Flask, request, jsonify
import requests
from groq import Groq
import json

app = Flask(__name__)

def deep_clean(obj):
    if isinstance(obj, str):
        return obj.encode('utf-8', 'ignore').decode('utf-8').replace('\u2028', ' ').replace('\u2029', ' ')
    elif isinstance(obj, list): return [deep_clean(item) for item in obj]
    elif isinstance(obj, dict): return {k: deep_clean(v) for k, v in obj.items()}
    return obj

@app.route('/api/analyze', methods=['POST'])
def analyze():
    try:
        raw_data = request.get_json(force=True)
        data = deep_clean(raw_data)
        
        p_token = data.get('pulsar_token')
        g_key = data.get('groq_key')
        s_id = str(data.get('search_id'))
        
        p_start = f"{data.get('date_from')}T00:00:00Z"
        p_end = f"{data.get('date_to')}T23:59:59Z"

        # 1. Fetch from Pulsar
        query = "query G($f:FilterInput!){results(filter:$f){results{content visibility engagement source publishedAt}}}"
        variables = {"f": {"searchIds": [s_id], "dateFrom": p_start, "dateTo": p_end}}
        
        p_res = requests.post("https://data.pulsarplatform.com/graphql/trac", 
                             json={"query": query, "variables": variables}, 
                             headers={"Authorization": f"Bearer {p_token}", "Content-Type": "application/json; charset=utf-8"})
        
        posts = deep_clean(p_res.json()).get('data', {}).get('results', {}).get('results', [])
        if not posts: return jsonify({"error": "No data found."}), 404

        # 2. Weighted Impact Sorting (Top 500)
        for p in posts: p['w'] = (p.get('visibility', 0) * 0.6) + (p.get('engagement', 0) * 0.4)
        top = sorted(posts, key=lambda x: x['w'], reverse=True)[:500]
        context = [{"text": p.get('content')[:160], "reach": p.get('visibility'), "eng": p.get('engagement'), "src": p.get('source'), "date": p.get('publishedAt')} for p in top]

        # 3. Generative UI Prompt
        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": """You are a Strategic Data Scientist. 
                Analyze the data and create a custom dashboard report.
                You MUST return a JSON with a 'widgets' array. 
                Each widget must be one of:
                1. { "type": "text", "title": "string", "body": "string" }
                2. { "type": "chart", "title": "string", "plotly_config": [Plotly.js data array] }
                3. { "type": "metric", "label": "string", "value": "string" }
                
                Be aggressive and insightful. If the user asks for brand comparisons, build charts that reflect that."""},
                {"role": "user", "content": f"User Goal: {data.get('prompt')}\n\nData: {json.dumps(context)}"}
            ]
        )

        return app.response_class(response=chat.choices[0].message.content, status=200, mimetype='application/json')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

from flask import Flask, request, jsonify
import requests
from groq import Groq
import json

app = Flask(__name__)

def deep_clean(obj):
    if isinstance(obj, str):
        return obj.encode('utf-8', 'ignore').decode('utf-8').replace('\u2028', ' ').replace('\u2029', ' ')
    elif isinstance(obj, list):
        return [deep_clean(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: deep_clean(v) for k, v in obj.items()}
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

        query = """
        query GetResults($filters:FilterInput!){
           results (filter:$filters){
               results { content visibility engagement source }
           }
        }
        """
        variables = {"filters": {"searchIds": [s_id], "dateFrom": p_start, "dateTo": p_end}}
        
        pulsar_res = requests.post(
            "https://data.pulsarplatform.com/graphql/trac", 
            json={"query": query, "variables": variables}, 
            headers={"Authorization": f"Bearer {p_token}", "Content-Type": "application/json; charset=utf-8"}
        )
        
        posts = deep_clean(pulsar_res.json()).get('data', {}).get('results', {}).get('results', [])
        if not posts: return jsonify({"error": "No data found."}), 404

        sources = [p.get('source', 'Unknown') for p in posts]
        sov = {s: round((sources.count(s) / len(sources)) * 100) for s in set(sources)}
        context = [{"text": p.get('content')[:200], "impact": p.get('visibility'), "src": p.get('source')} for p in sorted(posts, key=lambda x: x.get('visibility', 0), reverse=True)[:50]]

        client = Groq(api_key=g_key)
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "Return JSON: {executive_summary: string, themes: [{title: string, summary: string, impact: number, offensive: string, defensive: string}]}"},
                {"role": "user", "content": json.dumps(context, ensure_ascii=False)}
            ]
        )

        final_res = json.loads(completion.choices[0].message.content)
        final_res['sov'] = sov
        
        return app.response_class(
            response=json.dumps(deep_clean(final_res)),
            status=200,
            mimetype='application/json; charset=utf-8'
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

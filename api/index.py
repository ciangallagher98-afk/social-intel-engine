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
        
        # --- FIXED: The missing pulsar_res call ---
        pulsar_res = requests.post(
            "https://data.pulsarplatform.com/graphql/trac", 
            json={"query": query, "variables": variables}, 
            headers={"Authorization": f"Bearer {p_token}", "Content-Type": "application/json; charset=utf-8"}
        )
        
        # Scrub the response for Latin-1 safety immediately
        pulsar_json = deep_clean(pulsar_res.json())
        
        if 'errors' in pulsar_json:
            return jsonify({"error": pulsar_json['errors'][0]['message']}), 400

        posts = pulsar_json.get('data', {}).get('results', {}).get('results', [])
        if not posts: return jsonify({"error": "No data found for this range."}), 404

        # 1. Apply Weighted Impact Logic
        # Visibility (Reach) 60% + Engagement (Interactions) 40%
        for p in posts:
            p['impact_calc'] = (p.get('visibility', 0) * 0.6) + (p.get('engagement', 0) * 0.4)

        # 2. Sort by Weighted Impact and take Top 500
        top_posts = sorted(posts, key=lambda x: x['impact_calc'], reverse=True)[:500]

        # 3. Compress Context for the AI
        # Shortening content to ~180 chars to ensure 500 posts fit in the context window
        context = [{"t": p.get('content')[:180], "s": p.get('source'), "i": round(p['impact_calc'])} for p in top_posts]

        # 4. Generate Strategy
        client = Groq(api_key=g_key)
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": """Analyze 500 high-impact data points. 
                Identify the 4-5 most significant narrative themes.
                Return JSON: {
                    "executive_summary": "High-level overview",
                    "themes": [{"title": "...", "summary": "...", "impact": 1-100, "offensive": "...", "defensive": "..."}]
                }"""},
                {"role": "user", "content": f"Goal: {data.get('prompt')}\n\nData: {json.dumps(context)}"}
            ]
        )

        final_response = json.loads(completion.choices[0].message.content)
        
        # Calculate SOV based on the full search set
        sources = [p.get('source', 'Unknown') for p in posts]
        final_response['sov'] = {s: round((sources.count(s) / len(sources)) * 100) for s in set(sources)}
        
        return app.response_class(
            response=json.dumps(deep_clean(final_response)),
            status=200,
            mimetype='application/json; charset=utf-8'
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500

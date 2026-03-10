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
        
        # Extract brands from the prompt (e.g., "Smirnoff, Absolut")
        brands = [b.strip() for b in data.get('prompt', '').split(',') if len(b) > 2]
        if not brands: brands = ["Brand A", "Brand B"] # Fallback

        p_token, g_key, s_id = data.get('pulsar_token'), data.get('groq_key'), str(data.get('search_id'))
        p_start, p_end = f"{data.get('date_from')}T00:00:00Z", f"{data.get('date_to')}T23:59:59Z"

        query = "query G($f:FilterInput!){results(filter:$f){results{content visibility engagement source}}}"
        variables = {"f": {"searchIds": [s_id], "dateFrom": p_start, "dateTo": p_end}}
        
        p_res = requests.post("https://data.pulsarplatform.com/graphql/trac", 
                             json={"query": query, "variables": variables}, 
                             headers={"Authorization": f"Bearer {p_token}", "Content-Type": "application/json; charset=utf-8"})
        
        posts = deep_clean(p_res.json()).get('data', {}).get('results', {}).get('results', [])
        if not posts: return jsonify({"error": "No data found."}), 404

        # 1. Map Brands & Channels
        brand_map = {}
        for p in posts:
            content = p.get('content', '').lower()
            source = p.get('source', 'Web')
            matched = "Other"
            for b in brands:
                if b.lower() in content: matched = b; break
            
            if matched not in brand_map: brand_map[matched] = {"count": 0, "sources": {}}
            brand_map[matched]["count"] += 1
            brand_map[matched]["sources"][source] = brand_map[matched]["sources"].get(source, 0) + 1

        # 2. Weighted Impact Sorting
        for p in posts: p['w'] = (p.get('visibility', 0) * 0.6) + (p.get('engagement', 0) * 0.4)
        top = sorted(posts, key=lambda x: x['w'], reverse=True)[:500]
        context = [{"t": p.get('content')[:160], "s": p.get('source')} for p in top]

        # 3. AI Strategic Summary
        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "Return JSON: {executive_summary: string, themes: [{title, summary, impact, offensive, defensive}]}"},
                {"role": "user", "content": f"Analyze: {json.dumps(context)}"}
            ]
        )

        res = json.loads(chat.choices[0].message.content)
        res['brand_sov'] = brand_map # CRITICAL: Ensure this key exists
        return app.response_class(response=json.dumps(deep_clean(res)), status=200, mimetype='application/json')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

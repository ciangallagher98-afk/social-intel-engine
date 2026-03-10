from flask import Flask, request, jsonify
import requests
from groq import Groq
import json

app = Flask(__name__)

# Standard UTF-8 / Latin-1 safety
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
        brands = [b.strip() for b in data.get('prompt', '').split(',') if len(b) > 2] # User provides brands like "Smirnoff, Absolut"
        
        # ... (standard pulsar_res setup here) ...
        pulsar_res = requests.post(
            "https://data.pulsarplatform.com/graphql/trac", 
            json={"query": query, "variables": variables}, 
            headers={"Authorization": f"Bearer {p_token}", "Content-Type": "application/json; charset=utf-8"}
        )
        
        posts = deep_clean(pulsar_res.json()).get('data', {}).get('results', {}).get('results', [])
        if not posts: return jsonify({"error": "No data found."}), 404

        # 1. Advanced Brand & Source Mapping
        brand_data = {}
        for p in posts:
            content = p.get('content', '').lower()
            source = p.get('source', 'Web')
            # Assign post to a brand (first match logic)
            matched_brand = "General"
            for b in brands:
                if b.lower() in content:
                    matched_brand = b
                    break
            
            if matched_brand not in brand_data:
                brand_data[matched_brand] = {"count": 0, "sources": {}}
            
            brand_data[matched_brand]["count"] += 1
            brand_data[matched_brand]["sources"][source] = brand_data[matched_brand]["sources"].get(source, 0) + 1

        # 2. Strategic Sorting (Weighted Impact)
        for p in posts:
            p['impact_calc'] = (p.get('visibility', 0) * 0.6) + (p.get('engagement', 0) * 0.4)
        
        top_posts = sorted(posts, key=lambda x: x['impact_calc'], reverse=True)[:500]
        context = [{"t": p.get('content')[:160], "s": p.get('source'), "i": round(p['impact_calc'])} for p in top_posts]

        # 3. AI Analysis
        client = Groq(api_key=g_key)
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "Analyze competitor data. Identify 4 themes. For each theme provide: title, summary, impact, offensive, defensive."},
                {"role": "user", "content": f"Analyze: {data.get('prompt')}\n\nData: {json.dumps(context)}"}
            ]
        )

        final_response = json.loads(completion.choices[0].message.content)
        final_response['brand_sov'] = brand_data # Send the complex SOV map to the UI
        
        return app.response_class(
            response=json.dumps(deep_clean(final_response)),
            status=200, mimetype='application/json; charset=utf-8'
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

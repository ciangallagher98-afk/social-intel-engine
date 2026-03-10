from flask import Flask, request, jsonify
import requests
from groq import Groq
import json

app = Flask(__name__)

def deep_clean(obj):
    if isinstance(obj, str):
        return obj.encode('utf-8', 'ignore').decode('utf-8').replace('\u2028', ' ').replace('\u2029', ' ')
    return obj

@app.route('/api/analyze', methods=['POST'])
def analyze():
    try:
        raw_data = request.get_json(force=True)
        data = deep_clean(raw_data)
        
        # ... (standard setup for p_token, g_key, s_id, dates) ...

        query = """
        query GetResults($filters:FilterInput!){
           results (filter:$filters){
               results { content visibility engagement source }
           }
        }
        """
        # ... (pulsar_res logic) ...

        posts = pulsar_res.json().get('data', {}).get('results', {}).get('results', [])
        if not posts: return jsonify({"error": "No data found."}), 404

        # 1. Calculate Weighted Impact Score
        # Visibility (Reach) weighted at 60%, Engagement (Action) at 40%
        for p in posts:
            p['weighted_score'] = (p.get('visibility', 0) * 0.6) + (p.get('engagement', 0) * 0.4)

        # 2. Sort and take Top 500
        top_posts = sorted(posts, key=lambda x: x['weighted_score'], reverse=True)[:500]

        # 3. Compress Context (Max efficiency for 500 posts)
        # We only send the source and content to save space for the LLM
        context = [{"t": p.get('content')[:180], "s": p.get('source')} for p in top_posts]

        client = Groq(api_key=g_key)
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile", # Use the 70b for better reasoning on large datasets
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a Narrative Strategist. Analyze 500 high-impact posts. Look for deep patterns, emerging risks, and brand sentiment. Return JSON: {executive_summary, themes: [{title, summary, impact, offensive, defensive}]}"},
                {"role": "user", "content": f"Analyze these 500 data points for: {data.get('prompt')}\n\nData: {json.dumps(context)}"}
            ]
        )

        # ... (return response logic) ...

    except Exception as e:
        return jsonify({"error": str(e)}), 500

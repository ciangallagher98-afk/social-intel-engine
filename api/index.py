from flask import Flask, request, jsonify
import requests
from groq import Groq
import json

app = Flask(__name__)

def scrub_text(text):
    """Removes problematic Unicode characters like \u2028 and \u2029"""
    if not text: return ""
    # Replace line separators and paragraph separators with spaces
    return text.replace('\u2028', ' ').replace('\u2029', ' ').strip()

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
        
        raw_data = pulsar_res.json()
        if 'errors' in raw_data:
            return jsonify({"error": raw_data['errors'][0]['message']}), 400

        posts = raw_data.get('data', {}).get('results', {}).get('results', [])
        if not posts: return jsonify({"error": "No data found for this range."}), 404

        # 1. Share of Voice Calculation (All sources)
        sources = [p.get('source', 'Unknown') for p in posts]
        sov = {s: round((sources.count(s) / len(sources)) * 100) for s in set(sources)}

        # 2. Strategic Sampling (Top 50 by Impact)
        sorted_posts = sorted(posts, key=lambda x: x.get('visibility', 0), reverse=True)
        
        # Scrub text for safe UTF-8 encoding
        context = [{
            "text": scrub_text(p.get('content'))[:220], 
            "impact": p.get('visibility'), 
            "src": p.get('source'),
            "date": p.get('publishedAt')
        } for p in sorted_posts[:50]]

        # 3. AI Strategic Synthesis
        client = Groq(api_key=g_key)
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": """You are a Lead Strategy Consultant. 
                Identify the 4 most critical narrative themes. 
                For each theme, provide: 
                - title, summary, impact_score (1-100)
                - 'offensive_play': (Growth opportunity)
                - 'defensive_play': (Risk mitigation)
                Return JSON: {executive_summary, themes: []}"""},
                {"role": "user", "content": f"Goal: {data.get('prompt')}\n\nData: {json.dumps(context)}"}
            ]
        )

        result = json.loads(completion.choices[0].message.content)
        result['sov'] = sov # Append SOV to the AI response
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

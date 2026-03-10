from flask import Flask, request, jsonify
import requests
from groq import Groq
import json
import sys

app = Flask(__name__)

def deep_clean(obj):
    """Recursively force all strings in a dictionary/list to be safe UTF-8"""
    if isinstance(obj, str):
        # This removes \u2028, \u2029 and any other non-standard chars
        return obj.encode('utf-8', 'ignore').decode('utf-8').replace('\u2028', ' ').replace('\u2029', ' ')
    elif isinstance(obj, list):
        return [deep_clean(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: deep_clean(v) for k, v in obj.items()}
    return obj

@app.route('/api/analyze', methods=['POST'])
def analyze():
    try:
        # Layer 1: Clean incoming user prompt immediately
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
               results { content visibility engagement url publishedAt source }
           }
        }
        """
        variables = {"filters": {"searchIds": [s_id], "dateFrom": p_start, "dateTo": p_end}}
        
        # Layer 2: Secure the Pulsar Request
        pulsar_res = requests.post(
            "https://data.pulsarplatform.com/graphql/trac", 
            json={"query": query, "variables": variables}, 
            headers={
                "Authorization": f"Bearer {p_token}", 
                "Content-Type": "application/json; charset=utf-8" # Force UTF-8 Header
            }
        )
        
        # Layer 3: Deep clean the Pulsar Response
        pulsar_json = deep_clean(pulsar_res.json())
        
        posts = pulsar_json.get('data', {}).get('results', {}).get('results', [])
        if not posts: return jsonify({"error": "No data found."}), 404

        sources = [p.get('source', 'Unknown') for p in posts]
        sov = {s: round((sources.count(s) / len(sources)) * 100) for s in set(sources)}

        sorted_posts = sorted(posts, key=lambda x: x.get('visibility', 0), reverse=True)
        context = [{"text": p.get('content')[:200], "impact": p.get('visibility'), "src": p.get('source')} for p in sorted_posts[:50]]

        # Layer 4: Explicitly encode the AI payload
        client = Groq(api_key=g_key)
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a Strategy Consultant. Return JSON with executive_summary and themes array."},
                {"role": "user", "content": json.dumps(context, ensure_ascii=False)} # ensure_ascii=False keeps it as UTF-8
            ]
        )

        final_response = json.loads(completion.choices[0].message.content)
        final_response['sov'] = sov
        
        # Return with explicit mimetype to prevent server-side latin-1 encoding
        return app.response_class(
            response=json.dumps(deep_clean(final_response)),
            status=200,
            mimetype='application/json; charset=utf-8'
        )

    except Exception as e:
        print(f"Error Logged: {str(e)}") # Useful for debugging in console
        return jsonify({"error": "Encoding Error Handled: " + str(e)}), 500

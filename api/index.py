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
        
        # 1. Strict Type Casting for Pulsar Search ID
        try:
            search_id = int(data.get('search_id'))
        except:
            return jsonify({"error": "Search ID must be a number."}), 400

        # 2. ISO 8601 Date Formatting
        p_start = f"{data.get('date_from')}T00:00:00Z"
        p_end = f"{data.get('date_to')}T23:59:59Z"

        # 3. Robust GraphQL Query (Flattened engagement)
        query = """
        query GetResults($filters:FilterInput!){
           results (filter:$filters){
               results { 
                   content 
                   source 
                   visibility 
                   sentiment
                   emotion
                   engagement
                   contentUrl
               }
           }
        }
        """
        variables = {
            "filters": {
                "searchIds": [search_id], 
                "dateFrom": p_start, 
                "dateTo": p_end
            }
        }
        
        pulsar_res = requests.post(
            "https://data.pulsarplatform.com/graphql/trac", 
            json={"query": query, "variables": variables}, 
            headers={"Authorization": f"Bearer {p_token}", "Content-Type": "application/json"}
        )
        
        p_json = pulsar_res.json()
        
        # Handle Pulsar API level errors
        if 'errors' in p_json:
            return jsonify({"error": p_json['errors'][0]['message']}), 400

        posts = p_json.get('data', {}).get('results', {}).get('results', [])
        if not posts:
            return jsonify({"error": "No data found for this range."}), 404

        # 4. Enriched Context for Groq
        context_items = []
        for p in posts[:50]:
            context_items.append({
                "text": p.get('content', '')[:180],
                "sent": p.get('sentiment'),
                "emo": p.get('emotion'),
                "impact": p.get('visibility', 0),
                "eng": p.get('engagement', 0),
                "url": p.get('contentUrl', '')
            })

        # 5. Strategic Analysis with Llama 3.3
        client = Groq(api_key=g_key)
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": """Return ONLY a JSON object:
                {
                  "summary": "Executive summary of findings",
                  "categories": [
                    { "name": "Topic", "count": 0, "sentiment": "Text", "emotion": "Text", "boolean": "Text", "impact": 0, "url": "URL" }
                  ]
                }"""},
                {"role": "user", "content": f"Prompt: {data.get('prompt')}\n\nData: {json.dumps(context_items)}"}
            ]
        )

        return jsonify(json.loads(completion.choices[0].message.content))

    except Exception as e:
        return jsonify({"error": f"Internal Server Error: {str(e)}"}), 500

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
        s_id = data.get('search_id')
        user_prompt = data.get('prompt')
        date_from = data.get('date_from')
        date_to = data.get('date_to')

        # 1. Date Formatting
        p_start = f"{date_from}T00:00:00Z" if date_from else "2026-01-01T00:00:00Z"
        p_end = f"{date_to}T23:59:59Z" if date_to else "2026-12-31T23:59:59Z"

        # 2. Pulsar Fetch
        query = """
        query GetResults($filters:FilterInput!){
           results (filter:$filters){
               results { content, source }
           }
        }
        """
        variables = {"filters": {"searchIds": [s_id], "dateFrom": p_start, "dateTo": p_end}}
        headers = {"Authorization": f"Bearer {p_token}", "Content-Type": "application/json"}
        
        pulsar_res = requests.post("https://data.pulsarplatform.com/graphql/trac", 
                                    json={"query": query, "variables": variables}, headers=headers)
        posts = pulsar_res.json().get('data', {}).get('results', {}).get('results', [])

        if not posts:
            return jsonify({"error": "No data found for this range."}), 404

        # 3. Structured AI Analysis
        client = Groq(api_key=g_key)
        context_text = "\n".join([f"- {p.get('content')[:180]}" for p in posts[:40]])

        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": """Analyze social data and return ONLY a JSON object:
                {
                  "summary": "Short paragraph of insights",
                  "analysis_count": 50,
                  "categories": [
                    {
                      "name": "Category Name",
                      "count": 12,
                      "boolean": "pulsar AND boolean AND string"
                    }
                  ]
                }"""},
                {"role": "user", "content": f"Task: {user_prompt}\n\nData:\n{context_text}"}
            ]
        )

        # Parse the AI response and return to website
        ai_data = json.loads(completion.choices[0].message.content)
        return jsonify(ai_data)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

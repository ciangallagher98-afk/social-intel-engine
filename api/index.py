from flask import Flask, request, jsonify
import requests
from groq import Groq

app = Flask(__name__)

@app.route('/api/analyze', methods=['POST'])
def analyze():
    try:
        data = request.json
        # User inputs from the website
        p_token = data.get('pulsar_token')
        g_key = data.get('groq_key')
        s_id = data.get('search_id')
        user_prompt = data.get('prompt')
        date_from = data.get('date_from')
        date_to = data.get('date_to')

        # 1. Date Formatting (Fixes the "No data found" issue)
        # Pulsar requires ISO-8601 format: YYYY-MM-DDTHH:MM:SSZ
        p_start = f"{date_from}T00:00:00Z" if date_from else "2026-01-01T00:00:00Z"
        p_end = f"{date_to}T23:59:59Z" if date_to else "2026-12-31T23:59:59Z"

        # 2. Fetch from Pulsar
        # We fetch content and source for the AI to analyze
        query = """
        query GetResults($filters:FilterInput!){
           results (filter:$filters){
               results { 
                   content 
                   source 
               }
           }
        }
        """
        variables = {
            "filters": {
                "searchIds": [s_id], 
                "dateFrom": p_start,
                "dateTo": p_end
            }
        }
        headers = {
            "Authorization": f"Bearer {p_token}",
            "Content-Type": "application/json"
        }
        
        pulsar_res = requests.post(
            "https://data.pulsarplatform.com/graphql/trac", 
            json={"query": query, "variables": variables}, 
            headers=headers
        )
        
        # Log the response for debugging if needed
        res_json = pulsar_res.json()
        posts = res_json.get('data', {}).get('results', {}).get('results', [])

        if not posts:
            return jsonify({
                "error": f"No data found in Pulsar for Search ID {s_id} between {date_from} and {date_to}. Check your Search ID and ensure the date range contains data."
            }), 404

        # 3. Analyze with Groq AI
        client = Groq(api_key=g_key)
        
        # We take the first 30 posts to stay within AI token limits
        context_text = "\n".join([f"[{p.get('source')}] {p.get('content')[:200]}" for p in posts[:30]])
        
        # Use a default prompt if the user left it blank
        final_prompt = user_prompt if user_prompt and user_prompt.strip() else "Summarize the key themes and sentiment in these posts."

        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are a professional social media analyst. Be concise and insightful."},
                {"role": "user", "content": f"{final_prompt}\n\nData to analyze:\n{context_text}"}
            ]
        )

        return jsonify({
            "report": completion.choices[0].message.content,
            "count": len(posts)
        })
    
    except Exception as e:
        return jsonify({"error": f"System Error: {str(e)}"}), 500

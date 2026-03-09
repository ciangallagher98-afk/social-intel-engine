from flask import Flask, request, jsonify
import requests
from groq import Groq

app = Flask(__name__)

@app.route('/api/analyze', methods=['POST'])
def analyze():
    try:
        data = request.json
        # These come from the user's input on the website
        p_token = data.get('pulsar_token')
        g_key = data.get('groq_key')
        s_id = data.get('search_id')
        user_prompt = data.get('prompt', 'Analyze brand sentiment.')

        # 1. Fetch from Pulsar
        q = "query G($f:FilterInput!){results(filter:$f){results{content,source}}}"
        v = {"f": {"searchIds": [s_id]}} # Simplified date for demo
        headers = {"Authorization": f"Bearer {p_token}"}
        
        res = requests.post("https://data.pulsarplatform.com/graphql/trac", 
                            json={"query": q, "variables": v}, headers=headers)
        posts = res.json().get('data', {}).get('results', {}).get('results', [])

        if not posts:
            return jsonify({"error": "No data found in Pulsar"}), 404

        # 2. Analyze with AI
        client = Groq(api_key=g_key)
        context = "\n".join([p['content'][:150] for p in posts[:20]])
        
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": f"{user_prompt}\n\nData:\n{context}"}]
        )

        return jsonify({"report": chat.choices[0].message.content})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

from flask import Flask, request, jsonify
import requests
import json
from groq import Groq

app = Flask(__name__)
knowledge_base = {}

def clean_text(text):
    if not text: return ""
    return text.replace('\u2028', ' ').replace('\u2029', ' ').encode('utf-8', 'ignore').decode('utf-8')

@app.route('/api/ingest', methods=['POST'])
def ingest():
    try:
        data = request.get_json(force=True)
        s_id = str(data.get('search_id'))
        p_token = data.get('pulsar_token')
        d_from = data.get('from').split('.')[0] + "Z"
        d_to = data.get('to').split('.')[0] + "Z"

        # UPDATED: Flattened fields as per your Pulsar setup
        # Added sorting by VISIBILITY to prioritize high-reach posts
        query = """
        query GetPulsarData($f: FilterInput!) {
          results(filter: $f, limit: 250, offset: 0, sort: { field: VISIBILITY, order: DESC }) {
            results {
              content
              source
              visibility
              engagements
              sentiment
              emotions
              topics
            }
          }
        }
        """
        variables = {"f": {"searchIds": [int(s_id)], "dateFrom": d_from, "dateTo": d_to}}
        payload = json.dumps({"query": query, "variables": variables}).encode('utf-8')
        
        r = requests.post(
            "https://data.pulsarplatform.com/graphql/trac",
            data=payload,
            headers={
                "Authorization": f"Bearer {p_token}",
                "Content-Type": "application/json; charset=utf-8"
            },
            timeout=60
        )
        
        res_json = r.json()
        if "errors" in res_json:
            return jsonify({"error": res_json['errors'][0].get('message')}), 400

        batch = res_json.get('data', {}).get('results', {}).get('results', [])
        
        if not batch:
            return jsonify({"status": "empty", "message": "No data found. Check date range or ID."})

        # Sanitize text but keep the flattened analysis
        for post in batch:
            post['content'] = clean_text(post.get('content', ''))
            
        knowledge_base[s_id] = batch
        return jsonify({"status": "success", "count": len(batch)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/ask', methods=['POST'])
def ask():
    try:
        data = request.get_json(force=True)
        s_id, query, g_key = str(data.get('search_id')), data.get('question'), data.get('groq_key')

        dataset = knowledge_base.get(s_id, [])
        # We pass the flattened data directly to the LLM
        context = []
        for p in dataset[:150]:
            context.append({
                "text": p.get('content', '')[:150],
                "reach": p.get('visibility'),
                "sent": p.get('sentiment'),
                "emotions": p.get('emotions'),
                "topics": p.get('topics')
            })
            
        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are Gemini Intel. Analyze the highest visibility posts first. Identify trends in emotions and topics."},
                {"role": "user", "content": f"Data: {json.dumps(context)}\n\nQuestion: {query}"}
            ]
        )
        return jsonify({"answer": chat.choices[0].message.content})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

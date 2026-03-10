from flask import Flask, request, jsonify
import requests
import json
from groq import Groq

app = Flask(__name__)
knowledge_base = {}

def nuke_invisible_chars(text):
    if not text: return ""
    return str(text).replace('\u2028', '').replace('\u2029', '').replace('\n', '').strip()

@app.route('/api/ingest', methods=['POST'])
def ingest():
    try:
        data = request.get_json(force=True)
        s_id = nuke_invisible_chars(data.get('search_id'))
        p_token = nuke_invisible_chars(data.get('pulsar_token'))
        
        all_posts = []
        offset = 0
        limit = 50 
        max_pages = 20 # 50 posts * 20 pages = 1,000 deep

        while offset < (limit * max_pages):
            
            # THE PURE QUERY: Notice there is NO limit or offset here.
            # Using singular 'engagement' and 'emotion' based on your last working snippet.
            query = """
            query GetPulsarData($f: FilterInput!) {
              results(filter: $f) {
                results {
                  content
                  source
                  visibility
                  engagement
                  sentiment
                  emotion
                 
                }
              }
            }
            """
            
            # Paginating strictly through the FilterInput variables
            variables = {
                "f": {
                    "searchIds": [s_id],
                    "dateFrom": data.get('from'),
                    "dateTo": data.get('to'),
                    "limit": limit,
                    "offset": offset
                }
            }
            
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
                break # We hit the end of the data

            for post in batch:
                post['content'] = post.get('content', '').replace('\u2028', ' ').replace('\u2029', ' ')
            
            all_posts.extend(batch)
            
            if len(batch) < limit:
                break # Last page reached
                
            offset += limit

        if not all_posts:
            return jsonify({"status": "empty", "message": "Zero results. Check ID/Dates."})

        knowledge_base[s_id] = all_posts
        return jsonify({"status": "success", "count": len(all_posts)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/ask', methods=['POST'])
def ask():
    try:
        data = request.get_json(force=True)
        s_id = nuke_invisible_chars(data.get('search_id'))
        query = data.get('question')
        g_key = nuke_invisible_chars(data.get('groq_key'))

        dataset = knowledge_base.get(s_id, [])
        if not dataset:
            return jsonify({"answer": "Error: Knowledge base empty."}), 400

        # Sort all the collected posts by visibility
        sorted_dataset = sorted(dataset, key=lambda x: x.get('visibility', 0), reverse=True)

        # Context compression mapping to 'emotion' singular
        context = [{"text": p.get('content', '')[:140], "r": p.get('visibility'), "s": p.get('sentiment'), "e": p.get('emotion')} for p in sorted_dataset[:250]]
            
        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are Gemini Intelligence. Group insights by Emotion and Topic using Markdown."},
                {"role": "user", "content": f"Data: {json.dumps(context)}\n\nQuery: {query}"}
            ]
        )
        return jsonify({"answer": chat.choices[0].message.content})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)

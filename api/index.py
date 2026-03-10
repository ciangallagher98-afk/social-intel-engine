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
        
        variables = {
            "f": {
                "searchIds": [s_id],
                "dateFrom": data.get('from'),
                "dateTo": data.get('to')
            }
        }

        all_posts = []
        offset = 0
        limit = 50 # THE FIX: Pulsar strictly caps pagination at 50 per page!
        max_pages = 50 # 50 pages * 50 posts = 2,500 posts deep. Raise this if you want more.

        while offset < (limit * max_pages):
            # Your exact working query: limit and offset on the outer wrapper
            # Using 'engagement' and 'emotion' exactly as you wrote them
            query = """
            query GetPulsarData($f: FilterInput!) {
              results(filter: $f, limit: %d, offset: %d) {
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
            """ % (limit, offset)
            
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
                # If we get an empty batch, we've reached the very end of the data!
                break

            for post in batch:
                post['content'] = post.get('content', '').replace('\u2028', ' ').replace('\u2029', ' ')
            
            all_posts.extend(batch)
            
            # Since limit is 50, if it returns 49, we know it's the last page.
            if len(batch) < limit:
                break
                
            offset += limit # Turn the page

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

        # Updated context dict to match your working fields ('emotion' instead of 'emotions')
        context = [{"text": p.get('content', '')[:140], "r": p.get('visibility'), "s": p.get('sentiment'), "e": p.get('emotion')} for p in sorted_dataset[:250]]
            
        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are Gemini Intelligence. You are analyzing the highest-reach posts pulled from a massive dataset. Group insights by Emotion and Topic using Markdown."},
                {"role": "user", "content": f"Data: {json.dumps(context)}\n\nQuery: {query}"}
            ]
        )
        return jsonify({"answer": chat.choices[0].message.content})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)

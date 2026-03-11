from flask import Flask, request, jsonify
import requests
import json
from groq import Groq

app = Flask(__name__)

# Persistent in-memory storage for the session
knowledge_base = {}

def nuke_invisible_chars(text):
    """Deep cleans inputs to prevent HTTP header crashes from UI copy-pastes."""
    if not text: return ""
    return str(text).replace('\u2028', '').replace('\u2029', '').replace('\n', '').strip()

@app.route('/api/ingest', methods=['POST'])
def ingest():
    try:
        data = request.get_json(force=True)
        
        # 1. Clean UI Inputs
        s_id = nuke_invisible_chars(data.get('search_id'))
        p_token = nuke_invisible_chars(data.get('pulsar_token'))
        
        # 2. Setup Sliding Window Pagination Variables
        d_from = data.get('from')
        current_date_to = data.get('to')
        
        all_posts = []
        seen_content = set()
        pages_fetched = 0
        max_pages = 20 # Collects up to 1,000 posts (50 posts * 20 pages)
        
        while pages_fetched < max_pages:
            
            # The naked query with 'publishedAt' for time-sliding
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
                  topics
                  publishedAt
                }
              }
            }
            """
            
            variables = {
                "f": {
                    "searchIds": [s_id],
                    "dateFrom": d_from,
                    "dateTo": current_date_to
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
                break # End of timeline

            added_this_round = 0
            last_timestamp = None

            for post in batch:
                content = post.get('content', '')
                last_timestamp = post.get('publishedAt')
                
                # Deduplicate exact matches
                if content not in seen_content:
                    seen_content.add(content)
                    post['content'] = content.replace('\u2028', ' ').replace('\u2029', ' ')
                    all_posts.append(post)
                    added_this_round += 1
            
            # Pulsar max batch is 50. Less means we hit the end.
            if len(batch) < 50:
                break
                
            if added_this_round == 0 or not last_timestamp:
                break
                
            # Slide the window ceiling
            current_date_to = last_timestamp
            pages_fetched += 1

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
            return jsonify({"error": "Knowledge base empty. Run ingestion first."}), 400

        # Sort the accumulated posts by visibility
        sorted_dataset = sorted(dataset, key=lambda x: x.get('visibility', 0), reverse=True)

        # AI Context mapping: Reverted 'reach' back to 'visibility' to align with Pulsar's native naming
        context = [
            {
                "text": p.get('content', '')[:100], 
                "visibility": p.get('visibility'), 
                "sentiment": p.get('sentiment'), 
                "emotion": p.get('emotion')
            } for p in sorted_dataset[:50]
        ]
            
        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system", 
                    "content": "You are a strategic intelligence analyst. You are analyzing social media posts prioritized by their 'visibility' score. Answer the user's query directly and objectively using Markdown. Do NOT group your analysis by emotion or sentiment unless the user explicitly asks you to."
                },
                {
                    "role": "user", 
                    "content": f"Data: {json.dumps(context)}\n\nQuery: {query}"
                }
            ],
            temperature=0.3
        )
        return jsonify({"answer": chat.choices[0].message.content})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)

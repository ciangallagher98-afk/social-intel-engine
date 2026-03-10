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
        
        d_from = data.get('from')
        current_date_to = data.get('to')
        
        all_posts = []
        seen_content = set()
        pages_fetched = 0
        max_pages = 20 # Collects up to 1,000 posts (50 posts * 20 pages)
        
        while pages_fetched < max_pages:
            
            # THE PURE QUERY: Completely naked. No limits. No offsets.
            # Added 'publishedAt' so we can track our movement through time.
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
                  
                  publishedAt
                }
              }
            }
            """
            
            # The Sliding Window: 'dateTo' changes dynamically every loop
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
                break # We reached the end of the data

            added_this_round = 0
            last_timestamp = None

            for post in batch:
                content = post.get('content', '')
                last_timestamp = post.get('publishedAt')
                
                # Deduplicate overlapping posts on the exact same second boundary
                if content not in seen_content:
                    seen_content.add(content)
                    post['content'] = content.replace('\u2028', ' ').replace('\u2029', ' ')
                    all_posts.append(post)
                    added_this_round += 1
            
            # Pulsar's max batch size is 50. If we get less, we've hit the end.
            if len(batch) < 50:
                break
                
            # If the timestamp fails or we loop on duplicates, force break
            if added_this_round == 0 or not last_timestamp:
                break
                
            # Set the ceiling for the next fetch to the oldest post in this batch
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
            return jsonify({"answer": "Error: Knowledge base empty."}), 400

        # Sort the accumulated 1,000+ posts by visibility
        sorted_dataset = sorted(dataset, key=lambda x: x.get('visibility', 0), reverse=True)

        # Slice the top 250 highest-reach posts for the LLM
        context = [{"text": p.get('content', '')[:140], "r": p.get('visibility'), "s": p.get('sentiment'), "e": p.get('emotion'), "tp": p.get('topics')} for p in sorted_dataset[:250]]
            
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

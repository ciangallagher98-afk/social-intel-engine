from flask import Flask, request, jsonify
import requests
import json
from groq import Groq

app = Flask(__name__)

# Persistent in-memory storage for the session
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
        max_pages = 20 
        
        while pages_fetched < max_pages:
            
            # GRAPHQL UPDATE: Added 'url' to the requested fields
            query = """
            query GetPulsarData($f: FilterInput!) {
              results(filter: $f) {
                results {
                  content
                  url
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
                break 

            added_this_round = 0
            last_timestamp = None

            for post in batch:
                content = post.get('content', '')
                last_timestamp = post.get('publishedAt')
                
                if content not in seen_content:
                    seen_content.add(content)
                    post['content'] = content.replace('\u2028', ' ').replace('\u2029', ' ')
                    all_posts.append(post)
                    added_this_round += 1
            
            if len(batch) < 50:
                break
                
            if added_this_round == 0 or not last_timestamp:
                break
                
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

        sorted_dataset = sorted(dataset, key=lambda x: x.get('visibility', 0), reverse=True)

        # CONTEXT UPDATE: We now package the URL with the text so the LLM can cite it
        context = [
            {
                "text": p.get('content', '')[:100], 
                "url": p.get('url', 'No URL'),
                "visibility": p.get('visibility'), 
                "sentiment": p.get('sentiment'), 
                "emotion": p.get('emotion')
            } for p in sorted_dataset[:50]
        ]
            
        # SYSTEM PROMPT UPDATE: Strict instructions for citations and Boolean generation
        system_prompt = """
        You are a strategic intelligence analyst. Analyze the provided social media data objectively.
        
        Follow these strict rules:
        1. Answer the user's query directly using Markdown.
        2. When identifying a trend, narrative, or piece of analysis, provide 1-2 exact URLs from the provided data as 'Sample Evidence' linked in your response.
        3. Conclude your response with a 'Suggested Boolean Filter'. Write a standard boolean query (using AND, OR, "exact match") based on the keywords and narratives you identified, which the user can copy-paste into their tracking platform to monitor this specific trend.
        4. Do NOT group by emotion or sentiment unless asked.
        """

        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Data: {json.dumps(context)}\n\nQuery: {query}"}
            ],
            temperature=0.3
        )
        return jsonify({"answer": chat.choices[0].message.content})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)

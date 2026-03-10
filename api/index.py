from flask import Flask, request, jsonify, Response
import requests
import json
import time
from groq import Groq

app = Flask(__name__)
knowledge_base = {}

def safe_encode(text):
    """Deep cleans strings to prevent encoding crashes."""
    if not text: return ""
    # Remove problematic characters like \u2028 (Line Separator) and \u2029
    return text.replace('\u2028', ' ').replace('\u2029', ' ').encode('utf-8', 'ignore').decode('utf-8')

@app.route('/api/ingest', methods=['POST'])
def ingest():
    try:
        data = request.get_json(force=True)
        s_id = str(data.get('search_id'))
        p_token = data.get('pulsar_token')
        d_from, d_to = data.get('from'), data.get('to')
        
        def generate():
            total = 0
            knowledge_base[s_id] = []
            yield f"data: {json.dumps({'status': 'active', 'log': 'Connection Established...'})}\n\n"

            for page in range(20):
                offset = page * 50
                query = """
                query G($f:FilterInput!){
                  results(filter:$f, limit:50, offset:""" + str(offset) + """){
                    results {
                      content visibility engagement source publishedAt
                      analysis {
                        sentiment { label }
                        emotions { label }
                        topics { label }
                      }
                    }
                  }
                }
                """
                variables = {"f": {"searchIds": [s_id], "dateFrom": d_from, "dateTo": d_to}}
                
                try:
                    # FIX: Explicitly set encoding to utf-8 in the request
                    r = requests.post(
                        "https://data.pulsarplatform.com/graphql/trac", 
                        json={"query": query, "variables": variables}, 
                        headers={
                            "Authorization": f"Bearer {p_token}",
                            "Content-Type": "application/json; charset=utf-8"
                        },
                        timeout=45
                    )
                    
                    if r.status_code != 200:
                        yield f"data: {json.dumps({'status': 'error', 'log': f'API Error: {r.status_code}'})}\n\n"
                        break

                    # FIX: Ensure we read the response as UTF-8
                    r.encoding = 'utf-8'
                    res_json = r.json()
                    
                    batch = res_json.get('data', {}).get('results', {}).get('results', [])
                    if not batch: break
                    
                    # Clean the content of each post before storing
                    cleaned_batch = []
                    for post in batch:
                        post['content'] = safe_encode(post.get('content', ''))
                        cleaned_batch.append(post)

                    total += len(cleaned_batch)
                    knowledge_base[s_id].extend(cleaned_batch)
                    
                    progress = int(((page + 1) / 20) * 100)
                    yield f"data: {json.dumps({'status': 'ingesting', 'count': total, 'progress': progress, 'log': f'Indexed {total} posts...'})}\n\n"
                    time.sleep(0.4)
                    
                except Exception as inner_e:
                    # Log the specific error to the UI console
                    yield f"data: {json.dumps({'status': 'error', 'log': f'Parsing Error: {str(inner_e)}'})}\n\n"
                    break
                    
            yield f"data: {json.dumps({'status': 'complete', 'total': total})}\n\n"
        
        return Response(generate(), mimetype='text/event-stream')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/ask', methods=['POST'])
def ask():
    try:
        data = request.get_json(force=True)
        s_id, query, g_key = str(data.get('search_id')), data.get('question'), data.get('groq_key')
        
        dataset = knowledge_base.get(s_id, [])
        if not dataset: return jsonify({"error": "Knowledge base empty."}), 400

        # Create the AI context
        context = []
        for p in dataset[:500]:
            analysis = p.get('analysis', {}) or {}
            context.append({
                "t": p.get('content', '')[:160],
                "sent": analysis.get('sentiment', {}).get('label') if analysis.get('sentiment') else "N/A",
                "emo": [e.get('label') for e in analysis.get('emotions', [])[:1]],
                "topics": [t.get('label') for t in analysis.get('topics', [])[:2]]
            })
            
        client = Groq(api_key=g_key)
        chat = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are Gemini Intelligence. Analyze the provided dataset for trends in emotions and topics. Use Markdown."},
                {"role": "user", "content": f"Context: {json.dumps(context)}\n\nQuestion: {query}"}
            ]
        )
        return jsonify({"answer": chat.choices[0].message.content})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

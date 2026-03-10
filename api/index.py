# ... [imports and setup remain the same]

        # UPDATED QUERY: Removed { total } from engagement
        query = """
        query GetResults($filters:FilterInput!){
           results (filter:$filters){
               results { 
                   content 
                   source 
                   visibility 
                   sentiment
                   emotion
                   engagement
                   contentUrl
               }
           }
        }
        """
        # ... [variables and headers]
        
        pulsar_res = requests.post("https://data.pulsarplatform.com/graphql/trac", 
                                    json={"query": query, "variables": variables}, headers=headers)
        
        posts = pulsar_res.json().get('data', {}).get('results', {}).get('results', [])

        # Prepare enriched data for the LLM
        context_items = []
        for p in posts[:50]:
            context_items.append({
                "text": p.get('content')[:180],
                "sent": p.get('sentiment'),
                "emo": p.get('emotion'),
                "impact": p.get('visibility', 0),
                "eng": p.get('engagement', 0), # Now treating as a direct value
                "url": p.get('contentUrl')
            })

# ... [rest of the file remains the same]

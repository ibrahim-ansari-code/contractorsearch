import openai
import json
from typing import List, Dict, Any, Optional
from datetime import datetime

from config import settings

class RAGService:
    def __init__(self):
        self.client = openai.AsyncOpenAI(api_key=settings.openai_api_key)
        
    async def generate_answer(self, query, contractors):
        if not self.client:
            return {
                "answer": "error",
                "key_insights": [],
                "sources": [],
                "generated_at": datetime.utcnow().isoformat()
            }

        summaries = []
        for c in contractors:
            summary = f"- Name: {c.get('name', 'N/A')}"
            if c.get('city') and c.get('province'):
                summary += f", Location: {c['city']}, {c['province']}"
            if c.get('bio_text'):
                summary += f", Bio: {c['bio_text'][:100]}..."
            if c.get('services_text'):
                summary += f", Services: {c['services_text'][:100]}..."
            if c.get('hourly_rate_min') and c.get('hourly_rate_max'):
                summary += f", Rate: ${c['hourly_rate_min']}-${c['hourly_rate_max']}/hr"
            if c.get('has_license'):
                summary += ", Licensed: Yes"
            if c.get('has_insurance'):
                summary += ", Insured: Yes"
            summaries.append(summary)

        context_str = "\n".join(summaries)
        if not context_str:
            context_str = "No contractors"

        prompt = f"""
        You are an AI assistant for a contractor search engine. Answer the user's query based on the contractor information provided.

        Search Query: "{query}"

        Contractors:
        {context_str}

        Provide a natural language answer and extract 2-3 key insights. List contractor names as sources.

        Format as JSON:
        - "answer": (string) natural language answer
        - "key_insights": (list) 2-3 bullet points
        - "sources": (list) contractor names
        - "generated_at": (string) timestamp
        """

        try:
            # chat_completion = await self.openai_client.chat.completions.create(
            chat_completion = await self.client.chat.completions.create(
                messages=[
                    {"role": "system", "content": "You are a contractor search engine."},
                    {"role": "user", "content": prompt}
                ],
                model="gpt-3.5-turbo",
                response_format={"type": "json_object"}
            )
            
            response_content = chat_completion.choices[0].message.content
            print("got response")
            
            rag_response = json.loads(response_content)
            rag_response["generated_at"] = datetime.utcnow().isoformat()
            return rag_response

        except openai.APIError as e:
            print(f"openai error: {e}")
            return {
                "answer": f"AI error: {e.message}",
                "key_insights": [],
                "sources": [],
                "generated_at": datetime.utcnow().isoformat()
            }
        except json.JSONDecodeError as e:
            print(f"json error: {e}")
            return {
                "answer": "Error processing AI response",
                "key_insights": [],
                "sources": [],
                "generated_at": datetime.utcnow().isoformat()
            }
        except Exception as e:
            print(f"unexpected error: {e}")
            return {
                "answer": f"Error: {str(e)}",
                "key_insights": [],
                "sources": [],
                "generated_at": datetime.utcnow().isoformat()
            }
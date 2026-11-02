import os
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv


load_dotenv()
MY_API_KEY = os.getenv("OPENAI_API_KEY")
RAG_PROMPT = ChatPromptTemplate.from_template("""
Ты — узкоспециализированный ассистент по недвижимости. Твоя единственная база знаний — предоставленный ниже КОНТЕКСТ.

ПРАВИЛА ОТВЕТА:
1. Используй ТОЛЬКО информацию из блока КОНТЕКСТ.
2. Если в КОНТЕКСТЕ нет ответа на вопрос или подходящих квартир, ответь строго фразой: "Извините, в моей базе данных нет подходящих объявлений по вашему запросу."
3. Не дополняй ответ своими знаниями о географии Москвы, ценах или инфраструктуре, если этого нет в тексте объявлений.
4. Если контекст содержит информацию, отвечай кратко и по делу, указывая конкретные характеристики из текста.

КОНТЕКСТ:
{context}

ВОПРОС ПОЛЬЗОВАТЕЛЯ:
{question}

Твой ответ:""")


llm = ChatOpenAI(
    model="gpt-4o-mini", 
    openai_api_key= MY_API_KEY,
    openai_api_base="https://api.proxyapi.ru/openai/v1", 
    temperature=0 
)
# Тест
response = llm.invoke("Скажи 'Успех!'")
print(response.content)
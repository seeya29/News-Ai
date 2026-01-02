import datetime
import random
from typing import Any, Dict, List, Optional

class StubFetcher:
    """Simulates fetching from external agent pipelines (Gurukul, StockAgent, etc.).
    
    Generates mock items with appropriate categories and content to test the pipeline.
    """

    def fetch(self, agent_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        items = []
        # Simulate latency? No need for now.
        
        generator = self._get_generator(agent_name)
        for i in range(limit):
            item = generator(i)
            items.append(item)
        
        return items

    def _get_generator(self, agent_name: str):
        name = agent_name.lower()
        if "gurukul" in name:
            return self._gen_gurukul
        elif "stock" in name:
            return self._gen_stock
        elif "wellness" in name:
            return self._gen_wellness
        elif "car" in name:
            return self._gen_usedcar
        else:
            return self._gen_generic

    def _gen_gurukul(self, idx: int) -> Dict[str, Any]:
        topics = ["Python Asyncio", "RAG Pipelines", "Transformer Architecture", "Prompt Engineering"]
        topic = topics[idx % len(topics)]
        return {
            "title": f"Gurukul Lesson: Mastering {topic}",
            "body": f"In this lesson, we dive deep into {topic}. Understanding the core concepts is key to building scalable AI agents. We will cover best practices, common pitfalls, and real-world examples.",
            "link": f"https://gurukul.example.com/lesson/{idx}",
            "published_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "source": "GurukulAgent",
            "author": "Acharya AI",
            "category": "education"
        }

    def _gen_stock(self, idx: int) -> Dict[str, Any]:
        stocks = ["AAPL", "GOOGL", "TSLA", "MSFT", "NVDA"]
        stock = stocks[idx % len(stocks)]
        move = random.choice(["surges", "dips", "rallies", "stabilizes"])
        percent = random.randint(1, 15)
        return {
            "title": f"Market Update: {stock} {move} by {percent}%",
            "body": f"{stock} showed significant movement today amidst global market volatility. Analysts predict this trend might continue for the next quarter. Investors are advised to watch key support levels.",
            "link": f"https://stockagent.example.com/ticker/{stock}",
            "published_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "source": "StockAgent",
            "author": "MarketBot",
            "category": "finance"
        }

    def _gen_wellness(self, idx: int) -> Dict[str, Any]:
        tips = ["Hydration", "Meditation", "Sleep Hygiene", "Balanced Diet"]
        tip = tips[idx % len(tips)]
        return {
            "title": f"Daily Wellness: The Power of {tip}",
            "body": f"{tip} is essential for maintaining physical and mental health. Studies show that incorporating this into your daily routine improves longevity and reduces stress. Start small and build a habit.",
            "link": f"https://wellnessbot.example.com/tip/{idx}",
            "published_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "source": "WellnessBot",
            "author": "Dr. AI",
            "category": "health"
        }

    def _gen_usedcar(self, idx: int) -> Dict[str, Any]:
        cars = ["Toyota Camry", "Honda Civic", "Ford F-150", "Tesla Model 3"]
        car = cars[idx % len(cars)]
        year = 2018 + (idx % 5)
        price = 15000 + (idx * 1000)
        return {
            "title": f"Deal Alert: {year} {car} for ${price}",
            "body": f"Found a great deal on a {year} {car}. Low mileage, single owner, clean title. This vehicle is available now at our partner dealership. Contact us for a test drive.",
            "link": f"https://usedcar.example.com/listing/{idx}",
            "published_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "source": "UsedCarAgent",
            "author": "AutoScout",
            "category": "automotive"
        }

    def _gen_generic(self, idx: int) -> Dict[str, Any]:
        return {
            "title": f"Generic Agent Update #{idx}",
            "body": "This is a simulated update from an unknown agent. Content is placeholder.",
            "link": f"https://agent.example.com/update/{idx}",
            "published_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "source": "GenericAgent",
            "category": "general"
        }

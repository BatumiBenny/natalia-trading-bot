# package marker
# main.py - Python path რეგულატორი (სწრაფი ამოხსნა)

import sys
import os

# 🔧 დაამატე src ფოლდერი Python path
# ეს უხელი Python-ს იპოვის execution, indicators, filters ფოლდერი
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# ახლა შემდეგი იმპორტი მუშაობს:
# from execution.regime_engine import MarketRegimeEngine
# from indicators.rsi import RSI
# ... და სხვა

# თქვენი ორიგინალი კოდი აქ:
if __name__ == "__main__":
    # დაწყება ბოტი
    print("🚀 ბოტი იშვება...")
    
    # ... (თქვენი კოდი აქ)
    
    # მაგალითი:
    # from execution.regime_engine import MarketRegimeEngine
    # bot = MarketRegimeEngine()
    # bot.start()

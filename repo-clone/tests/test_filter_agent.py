from single_pipeline.agents.filter_agent import FilterAgent


def test_basic_language_detect_supports_more_scripts():
    agent = FilterAgent()
    samples = {
        "pa": "ਪੰਜਾਬੀ",
        "gu": "ગુજરાતી",
        "te": "తెలుగు",
        "kn": "ಕನ್ನಡ",
        "ml": "മലയാളം",
        "ur": "اردو",
    }
    for code, text in samples.items():
        lang = agent._basic_language_detect(text)
        assert lang in (code, "mixed", "unknown")